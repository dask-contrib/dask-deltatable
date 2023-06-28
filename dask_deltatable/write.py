from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Mapping

import dask.dataframe as dd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from dask.core import flatten
from dask.dataframe.core import Scalar
from dask.highlevelgraph import HighLevelGraph
from deltalake import DeltaTable
from deltalake.writer import (
    MAX_SUPPORTED_WRITER_VERSION,
    PYARROW_MAJOR_VERSION,
    AddAction,
    DeltaJSONEncoder,
    DeltaProtocolError,
    DeltaStorageHandler,
    __enforce_append_only,
    _write_new_deltalake,
    get_file_stats_from_metadata,
    get_partitions_from_path,
    try_get_table_and_table_uri,
)
from pyarrow.lib import RecordBatchReader
from toolz.itertoolz import pluck

from ._schema import validate_compatible


def to_deltalake(
    table_or_uri: str | Path | DeltaTable,
    df: dd.DataFrame,
    *,
    schema: pa.Schema | None = None,
    partition_by: list[str] | str | None = None,
    filesystem: pa_fs.FileSystem | None = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    file_options: ds.ParquetFileWriteOptions | None = None,
    max_partitions: int | None = None,
    max_open_files: int = 1024,
    max_rows_per_file: int = 10 * 1024 * 1024,
    min_rows_per_group: int = 64 * 1024,
    max_rows_per_group: int = 128 * 1024,
    name: str | None = None,
    description: str | None = None,
    configuration: Mapping[str, str | None] | None = None,
    overwrite_schema: bool = False,
    storage_options: dict[str, str] | None = None,
    partition_filters: list[tuple[str, str, Any]] | None = None,
):
    """Write a given dask.DataFrame to a delta table

    TODO:
    """
    table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)

    # We need to write against the latest table version
    if table:
        table.update_incremental()

    __enforce_append_only(table=table, configuration=configuration, mode=mode)

    if filesystem is None:
        if table is not None:
            storage_options = table._storage_options or {}
            storage_options.update(storage_options or {})

        filesystem = pa_fs.PyFileSystem(DeltaStorageHandler(table_uri, storage_options))

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    if table:  # already exists
        if schema != table.schema().to_pyarrow() and not (
            mode == "overwrite" and overwrite_schema
        ):
            raise ValueError(
                "Schema of data does not match table schema\n"
                f"Table schema:\n{schema}\nData Schema:\n{table.schema().to_pyarrow()}"
            )

        if mode == "error":
            raise AssertionError("DeltaTable already exists.")
        elif mode == "ignore":
            return

        current_version = table.version()

        if partition_by:
            assert partition_by == table.metadata().partition_columns
        else:
            partition_by = table.metadata().partition_columns

        if table.protocol().min_writer_version > MAX_SUPPORTED_WRITER_VERSION:
            raise DeltaProtocolError(
                "This table's min_writer_version is "
                f"{table.protocol().min_writer_version}, "
                "but this method only supports version 2."
            )
    else:  # creating a new table
        current_version = -1

    # FIXME: schema is only known at this point if provided by the user
    if partition_by and schema:
        partition_schema = pa.schema([schema.field(name) for name in partition_by])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
    else:
        if partition_by:
            raise NotImplementedError("Have to provide schema when using partition_by")
        partitioning = None
    if mode == "overwrite":
        # FIXME: There are a couple of checks that are not migrated yet
        raise NotImplementedError()

    written = df.map_partitions(
        _write_partition,
        schema=schema,
        partitioning=partitioning,
        current_version=current_version,
        file_options=file_options,
        max_open_files=max_open_files,
        max_rows_per_file=max_rows_per_file,
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        filesystem=filesystem,
        max_partitions=max_partitions,
        meta=(None, object),
    )
    final_name = "delta-commit"
    dsk = {
        (final_name, 0): (
            _commit,
            table,
            written.__dask_keys__(),
            table_uri,
            schema,
            mode,
            partition_by,
            name,
            description,
            configuration,
            storage_options,
            partition_filters,
        )
    }
    graph = HighLevelGraph.from_collections(final_name, dsk, dependencies=(written,))
    return Scalar(graph, final_name, "")


def _commit(
    table,
    schemas_add_actions_nested,
    table_uri,
    schema,
    mode,
    partition_by,
    name,
    description,
    configuration,
    storage_options,
    partition_filters,
):
    schemas = list(flatten(pluck(0, schemas_add_actions_nested)))
    add_actions = list(flatten(pluck(1, schemas_add_actions_nested)))
    # TODO: What should the behavior be if the schema is provided? Cast the
    # data?
    if schema:
        schemas.append(schema)

    # TODO: This is applying a potentially stricted schema control than what
    # Delta requires but if this passes, it should be good to go
    schema = validate_compatible(schemas)
    assert schema
    if table is None:
        _write_new_deltalake(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or [],
            name,
            description,
            configuration,
            storage_options,
        )
    else:
        table._table.create_write_transaction(
            add_actions,
            mode,
            partition_by or [],
            schema,
            partition_filters,
        )
        table.update_incremental()


def _write_partition(
    df,
    *,
    schema,
    partitioning,
    current_version,
    file_options,
    max_open_files,
    max_rows_per_file,
    min_rows_per_group,
    max_rows_per_group,
    filesystem,
    max_partitions,
) -> tuple[pa.Schema, list[AddAction]]:
    # TODO: what to do with the schema, if provided
    data = pa.Table.from_pandas(df)
    schema = schema or data.schema

    add_actions: list[AddAction] = []

    def visitor(written_file: Any) -> None:
        path, partition_values = get_partitions_from_path(written_file.path)
        stats = get_file_stats_from_metadata(written_file.metadata)

        # PyArrow added support for written_file.size in 9.0.0
        if PYARROW_MAJOR_VERSION >= 9:
            size = written_file.size
        else:
            size = filesystem.get_file_info([path])[0].size

        add_actions.append(
            AddAction(
                path,
                size,
                partition_values,
                int(datetime.now().timestamp() * 1000),
                True,
                json.dumps(stats, cls=DeltaJSONEncoder),
            )
        )

    ds.write_dataset(
        data,
        base_dir="/",
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        # It will not accept a schema if using a RBR
        schema=schema if not isinstance(data, RecordBatchReader) else None,
        existing_data_behavior="overwrite_or_ignore",
        file_options=file_options,
        max_open_files=max_open_files,
        file_visitor=visitor,
        max_rows_per_file=max_rows_per_file,
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        filesystem=filesystem,
        max_partitions=max_partitions,
    )
    return schema, add_actions