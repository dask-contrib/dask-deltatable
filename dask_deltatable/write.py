from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

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
from toolz.itertoolz import pluck

from ._schema import pyarrow_to_deltalake, validate_compatible


def to_deltalake(
    table_or_uri: str | Path | DeltaTable,
    df: dd.DataFrame,
    *,
    schema: pa.Schema | None = None,
    partition_by: list[str] | str | None = None,
    filesystem: pa_fs.FileSystem | None = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    file_options: Mapping[str, Any] | None = None,
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
    compute: bool = True,
):
    """Write a given dask.DataFrame to a delta table. The returned value is a Dask Scalar,
    and the writing operation is only triggered when calling ``.compute()``

    Parameters
    ----------
    table_or_uri: str | Path | DeltaTable
        URI of a table or a DeltaTable object.
    df: dd.DataFrame
        Data to write
    schema : pa.Schema | None. Default None
        Optional schema to write.
    partition_by : list[str] | str | None. Default None
        List of columns to partition the table by. Only required
        when creating a new table
    filesystem : pa_fs.FileSystem | None. Default None
        Optional filesystem to pass to PyArrow. If not provided will
        be inferred from uri. The file system has to be rooted in the table root.
        Use the pyarrow.fs.SubTreeFileSystem, to adopt the root of pyarrow file systems.
    mode : Literal["error", "append", "overwrite", "ignore"]. Default "error"
        How to handle existing data. Default is to error if table already exists.
        If 'append', will add new data.
        If 'overwrite', will replace table with new data.
        If 'ignore', will not write anything if table already exists.
    file_options : Mapping[str, Any] | None. Default None
        Optional dict of options that can be used to initialize ParquetFileWriteOptions.
        Please refer to https://github.com/apache/arrow/blob/master/python/pyarrow/_dataset_parquet.pyx
        for the list of available options
    max_partitions : int | None. Default None
        The maximum number of partitions that will be used.
    max_open_files : int. Default 1024
        Limits the maximum number of
        files that can be left open while writing. If an attempt is made to open
        too many files then the least recently used file will be closed.
        If this setting is set too low you may end up fragmenting your
        data into many small files.
    max_rows_per_file : int. Default 10 * 1024 * 1024
        Maximum number of rows per file.
        If greater than 0 then this will limit how many rows are placed in any single file.
        Otherwise there will be no limit and one file will be created in each output directory
        unless files need to be closed to respect max_open_files
    min_rows_per_group : int. Default 64 * 1024
        Minimum number of rows per group. When the value is set,
        the dataset writer will batch incoming data and only write the row groups to the disk
        when sufficient rows have accumulated.
    max_rows_per_group : int. Default 128 * 1024
        Maximum number of rows per group.
        If the value is set, then the dataset writer may split up large incoming batches into multiple row groups.
        If this value is set, then min_rows_per_group should also be set
    name: str | None. Default None
        User-provided identifier for this table.
    description : str | None. Default None
        User-provided description for this table
    configuration : Mapping[str, str | None] | None. Default None
        A map containing configuration options for the metadata action.
    overwrite_schema : bool. Default False
        If True, allows updating the schema of the table.
    storage_options : dict[str, str] | None. Default None
        Options passed to the native delta filesystem. Unused if 'filesystem' is defined
    partition_filters : list[tuple[str, str, Any]] | None. Default None
        The partition filters that will be used for partition overwrite.
    compute : bool. Default True
        Whether to trigger the writing operation immediately

    Returns
    -------
    dask.Scalar
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

    if schema is not None:
        schema = pyarrow_to_deltalake(schema)

    if table:  # already exists
        if (
            schema is not None
            and schema != table.schema().to_pyarrow()
            and not (mode == "overwrite" and overwrite_schema)
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
                f"but this method only supports version {MAX_SUPPORTED_WRITER_VERSION}."
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
        raise NotImplementedError("mode='overwrite' is not implemented")

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
    result = Scalar(graph, final_name, "")
    if compute:
        result = result.compute()
    return result


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
    if schema is None:
        #
        schema = pyarrow_to_deltalake(pa.Schema.from_pandas(df))
    data = pa.Table.from_pandas(df, schema=schema)

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

    if file_options is not None:
        file_options = ds.ParquetFileFormat().make_write_options(**file_options)

    ds.write_dataset(
        data,
        base_dir="/",
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        # It will not accept a schema if using a RBR
        schema=schema,
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
