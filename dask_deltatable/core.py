from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any, Callable, cast

import dask
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.base import tokenize
from dask.dataframe.io.parquet.arrow import ArrowDatasetEngine
from dask.dataframe.utils import make_meta
from deltalake import DeltaTable
from fsspec.core import get_fs_token_paths
from packaging.version import Version
from pyarrow import dataset as pa_ds

from . import utils
from .types import Filters

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
    partition_filters = utils.get_partition_filters(
        dt.metadata().partition_columns, filter
    )
    if not partition_filters:
        # can't filter
        return sorted(dt.file_uris())
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
    pyarrow_to_pandas["types_mapper"] = _get_type_mapper(
        pyarrow_to_pandas.get("types_mapper")
    )
    pyarrow_to_pandas["ignore_metadata"] = pyarrow_to_pandas.get(
        "ignore_metadata", False
    )
    table = pa_ds.dataset(
        source=filename,
        schema=schema,
        filesystem=fs,
        format="parquet",
        partitioning="hive",
    ).to_table(filter=filter_expression, columns=columns)
    return table.to_pandas(**pyarrow_to_pandas)


def _read_from_filesystem(
    path: str,
    version: int | None,
    columns: Sequence[str] | None,
    datetime: str | None = None,
    storage_options: dict[str, str] | None = None,
    delta_storage_options: dict[str, str] | None = None,
    **kwargs: dict[str, Any],
) -> dd.DataFrame:
    """
    Reads the list of parquet files in parallel
    """
    storage_options = utils.maybe_set_aws_credentials(path, storage_options)  # type: ignore
    delta_storage_options = utils.maybe_set_aws_credentials(path, delta_storage_options)  # type: ignore

    fs, fs_token, _ = get_fs_token_paths(path, storage_options=storage_options)
    dt = DeltaTable(
        table_uri=path, version=version, storage_options=delta_storage_options
    )
    if datetime is not None:
        dt.load_as_version(datetime)

    schema = pa.schema(dt.schema())

    filter_value = cast(Filters, kwargs.get("filter", None))
    pq_files = _get_pq_files(dt, filter=filter_value)

    mapper_kwargs = kwargs.get("pyarrow_to_pandas", {})
    mapper_kwargs["types_mapper"] = _get_type_mapper(
        mapper_kwargs.get("types_mapper", None)
    )
    meta = make_meta(pa.table(schema.empty_table()).to_pandas(**mapper_kwargs))
    if columns:
        meta = meta[columns]

    if not dd._dask_expr_enabled():
        # Setting token not supported in dask-expr
        kwargs["token"] = tokenize(path, fs_token, **kwargs)  # type: ignore

    if len(pq_files) == 0:
        df = schema.empty_table().to_pandas()
        if columns is not None:
            df = df[columns]
        return dd.from_pandas(df, npartitions=1)
    else:
        return dd.from_map(
            _read_delta_partition,
            pq_files,
            fs=fs,
            columns=columns,
            schema=schema,
            meta=meta,
            label="read-delta-table",
            **kwargs,
        )


def _get_type_mapper(
    user_types_mapper: dict[str, Any] | None,
) -> Callable[[Any], Any] | None:
    """
    Set the type mapper for the schema
    """
    convert_string = dask.config.get("dataframe.convert-string", True)
    if convert_string is None:
        convert_string = True
    return ArrowDatasetEngine._determine_type_mapper(
        dtype_backend=None,
        convert_string=convert_string,
        arrow_to_pandas={"types_mapper": user_types_mapper},
    )


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
        if catalog is provided, user has to provide database and table name, and
        delta-rs will fetch the metadata from glue catalog, this is used by dask to read
        the parquet tables
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

        schema: pyarrow.Schema
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
            raise NotImplementedError(
                "Reading from a catalog used to be supported ",
                "but was removed from the upstream dependency delta-rs>=1.0.",
            )
    else:
        if path is None:
            raise ValueError("Please Provide Delta Table path")

        delta_storage_options = utils.maybe_set_aws_credentials(path, delta_storage_options)  # type: ignore
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


def read_unity_catalog(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    **kwargs,
) -> dd.DataFrame:
    """
    Read a Delta Table from Databricks Unity Catalog into a Dask DataFrame.

    This function connects to Databricks using the WorkspaceClient and retrieves
    temporary credentials to access the specified Unity Catalog table. It then
    reads the Delta table's Parquet files into a Dask DataFrame.

    Parameters
    ----------
    catalog_name : str
        Name of the Unity Catalog catalog.
    schema_name : str
        Name of the schema within the catalog.
    table_name : str
        Name of the table within the catalog schema.
    **kwargs
        Additional keyword arguments passed to `dask.dataframe.read_parquet`.
        Some most used parameters can be passed here are:
        1. schema
        2. filter
        3. pyarrow_to_pandas
        4. databricks_host
        5. databricks_token

        schema: pyarrow.Schema
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

        databricks_host: str
            The Databricks workspace URL hosting the Unity Catalog.

        databricks_token: str
            A Databricks personal access token with at least read access on the catalog.

    Returns
    -------
    dask.dataframe.DataFrame
        A Dask DataFrame representing the Delta table.

    Notes
    -----
    Requires the following to be set as either environment variables or in `kwargs` as
    lower case:
    - DATABRICKS_HOST: The Databricks workspace URL hosting the Unity Catalog.
    - DATABRICKS_TOKEN: A Databricks personal access token with at least read access on
        the catalog.

    Example
    -------
    >>> ddf = read_unity_catalog(
            catalog_name="main",
            database_name="my_db",
            able_name="my_table",
        )
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import TableOperation

    try:
        workspace_client = WorkspaceClient(
            host=os.environ.get("DATABRICKS_HOST", kwargs["databricks_host"]),
            token=os.environ.get("DATABRICKS_TOKEN", kwargs["databricks_token"]),
        )
    except KeyError:
        raise ValueError(
            "Please set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` either as environment"
            " variables or as part of `kwargs` with lowercase"
        )
    uc_full_url = f"{catalog_name}.{schema_name}.{table_name}"
    table = workspace_client.tables.get(uc_full_url)
    temp_credentials = workspace_client.temporary_table_credentials.generate_temporary_table_credentials(
        operation=TableOperation.READ,
        table_id=table.table_id,
    )
    storage_options = {
        "sas_token": temp_credentials.azure_user_delegation_sas.sas_token
    }
    delta_table = DeltaTable(
        table_uri=table.storage_location, storage_options=storage_options
    )
    ddf = dd.read_parquet(
        path=delta_table.file_uris(),
        storage_options=storage_options,
        **kwargs,
    )
    return ddf
