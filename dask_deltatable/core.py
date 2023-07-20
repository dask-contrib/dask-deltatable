from __future__ import annotations

import os
from collections.abc import Sequence
from functools import partial
from typing import Any, cast

import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.base import tokenize
from dask.dataframe.utils import make_meta
from deltalake import DataCatalog, DeltaTable
from fsspec.core import get_fs_token_paths
from packaging.version import Version
from pyarrow import dataset as pa_ds

from .types import Filters
from .utils import get_partition_filters

if Version(pa.__version__) >= Version("10.0.0"):
    filters_to_expression = pq.filters_to_expression
else:
    # fallback to older internal method
    filters_to_expression = pq._filters_to_expression


def _get_pq_files(dt: DeltaTable, filter: Filters = None) -> list[str]:
    """
    Get the list of parquet files after loading the
    current datetime version

    Parameters
    ----------
    dt : DeltaTable
        DeltaTable instance
    filter : list[tuple[str, str, Any]] | list[list[tuple[str, str, Any]]] | None
        Filters in DNF form.

    Returns
    -------
    list[str]
        List of files matching optional filter.
    """
    partition_filters = get_partition_filters(dt.metadata().partition_columns, filter)
    if not partition_filters:
        # can't filter
        return dt.file_uris()
    file_uris = set()
    for filter_set in partition_filters:
        file_uris.update(dt.file_uris(partition_filters=filter_set))
    return sorted(list(file_uris))


def _read_delta_partition(
    filename: str,
    schema: pa.Schema,
    fs: Any,
    columns: Sequence[str] | None,
    filter: Filters = None,
    pyarrow_to_pandas: dict[str, Any] | None = None,
    **_kwargs: dict[str, Any],
):
    filter_expression = filters_to_expression(filter) if filter else None
    if pyarrow_to_pandas is None:
        pyarrow_to_pandas = {}
    return (
        pa_ds.dataset(
            source=filename,
            schema=schema,
            filesystem=fs,
            format="parquet",
            partitioning="hive",
        )
        .to_table(filter=filter_expression, columns=columns)
        .to_pandas(**pyarrow_to_pandas)
    )


def _read_from_filesystem(
    path: str,
    version: int | None,
    columns: Sequence[str] | None,
    datetime: str | None = None,
    storage_options: dict[str, str] | None = None,
    delta_storage_options: dict[str, str] | None = None,
    **kwargs: dict[str, Any],
) -> dd.core.DataFrame:
    """
    Reads the list of parquet files in parallel
    """
    fs, fs_token, _ = get_fs_token_paths(path, storage_options=storage_options)
    dt = DeltaTable(
        table_uri=path, version=version, storage_options=delta_storage_options
    )
    if datetime is not None:
        dt.load_with_datetime(datetime)

    schema = dt.schema().to_pyarrow()

    filter_value = cast(Filters, kwargs.get("filter", None))
    pq_files = _get_pq_files(dt, filter=filter_value)
    if len(pq_files) == 0:
        raise RuntimeError("No Parquet files are available")

    mapper_kwargs = kwargs.get("pyarrow_to_pandas", {})
    meta = make_meta(schema.empty_table().to_pandas(**mapper_kwargs))
    if columns:
        meta = meta[columns]

    return dd.from_map(
        partial(_read_delta_partition, fs=fs, columns=columns, schema=schema, **kwargs),
        pq_files,
        meta=meta,
        label="read-delta-table",
        token=tokenize(fs_token, **kwargs),
    )


def _read_from_catalog(
    database_name: str, table_name: str, **kwargs
) -> dd.core.DataFrame:
    if ("AWS_ACCESS_KEY_ID" not in os.environ) and (
        "AWS_SECRET_ACCESS_KEY" not in os.environ
    ):
        # defer's installing boto3 upfront !
        from boto3 import Session

        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = current_credentials.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = current_credentials.secret_key
    data_catalog = DataCatalog.AWS
    dt = DeltaTable.from_data_catalog(
        data_catalog=data_catalog, database_name=database_name, table_name=table_name
    )

    df = dd.read_parquet(dt.file_uris(), **kwargs)
    return df


def read_deltalake(
    path: str | None = None,
    catalog: str | None = None,
    database_name: str | None = None,
    table_name: str | None = None,
    version: int | None = None,
    columns: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
    datetime: str | None = None,
    delta_storage_options: dict[str, str] | None = None,
    **kwargs,
):
    """
    Read a Delta Table into a Dask DataFrame

    This reads a list of Parquet files in delta table directory into a
    Dask.dataframe.

    Parameters
    ----------
    path: Optional[str]
        path of Delta table directory
    catalog: Optional[str]
        Currently supports only AWS Glue Catalog
        if catalog is provided, user has to provide database and table name, and delta-rs will fetch the
        metadata from glue catalog, this is used by dask to read the parquet tables
    database_name: Optional[str]
        database name present in the catalog
    tablename: Optional[str]
        table name present in the database of the Catalog
    version: int, default None
        DeltaTable Version, used for Time Travelling across the
        different versions of the parquet datasets
    datetime: str, default None
        Time travel Delta table to the latest version that's created at or
        before provided `datetime_string` argument.
        The `datetime_string` argument should be an RFC 3339 and ISO 8601 date
         and time string.

        Examples:
        `2018-01-26T18:30:09Z`
        `2018-12-19T16:39:57-08:00`
        `2018-01-26T18:30:09.453+00:00`
         #(copied from delta-rs docs)
    columns: None or list(str)
        Columns to load. If None, loads all.
    storage_options : dict, default None
        Key/value pairs to be passed on to the fsspec backend, if any.
    delta_storage_options : dict, default None
        Key/value pairs to be passed on to the delta-rs filesystem, if any.
    kwargs: dict,optional
        Some most used parameters can be passed here are:
        1. schema
        2. filter
        3. pyarrow_to_pandas

        schema : pyarrow.Schema
            Used to maintain schema evolution in deltatable.
            delta protocol stores the schema string in the json log files which is
            converted into pyarrow.Schema and used for schema evolution
            i.e Based on particular version, some columns can be
            shown or not shown.

        filter: Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
            List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
            Can act as both partition as well as row based filter, above list of filters
            converted into pyarrow.dataset.Expression built using pyarrow.dataset.Field
            example:
                [("x",">",400)] --> pyarrow.dataset.field("x")>400

        pyarrow_to_pandas: dict
            Options to pass directly to pyarrow.Table.to_pandas.
            Common options include:
            * categories: list[str]
                List of columns to treat as pandas.Categorical
            * strings_to_categorical: bool
                Encode string (UTF8) and binary types to pandas.Categorical.
            * types_mapper: Callable
                A function mapping a pyarrow DataType to a pandas ExtensionDtype

            See https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas
            for more.

    Returns
    -------
    Dask.DataFrame

    Examples
    --------
    >>> import dask_deltatable as ddt
    >>> df = ddt.read_deltalake('s3://bucket/my-delta-table')  # doctest: +SKIP

    """
    if catalog is not None:
        if (database_name is None) or (table_name is None):
            raise ValueError(
                "Since Catalog was provided, please provide Database and table name"
            )
        else:
            resultdf = _read_from_catalog(
                database_name=database_name, table_name=table_name, **kwargs
            )
    else:
        if path is None:
            raise ValueError("Please Provide Delta Table path")
        resultdf = _read_from_filesystem(
            path=path,
            version=version,
            columns=columns,
            storage_options=storage_options,
            datetime=datetime,
            delta_storage_options=delta_storage_options,
            **kwargs,
        )
    return resultdf
