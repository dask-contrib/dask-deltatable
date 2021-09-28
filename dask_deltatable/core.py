import json
import os
from urllib.parse import urlparse

import dask
import pyarrow.parquet as pq
from dask.base import tokenize
from dask.dataframe.io import from_delayed
from dask.delayed import delayed
from deltalake import DeltaTable
from fsspec.core import get_fs_token_paths
from pyarrow import dataset as pa_ds


class DeltaTableWrapper(object):
    def __init__(
        self, path, version, columns, datetime=None, storage_options=None
    ) -> None:
        self.path = path
        self.version = version
        self.columns = columns
        self.datetime = datetime
        self.storage_options = storage_options
        self.dt = DeltaTable(table_uri=self.path, version=self.version)
        self.fs, self.fs_token, _ = get_fs_token_paths(
            path, storage_options=storage_options
        )
        self.schema = self.dt.pyarrow_schema()

    def read_delta_dataset(self, f, **kwargs):
        schema = kwargs.pop("schema", None) or self.schema
        filter = kwargs.pop("filter", None)
        if filter:
            filter_expression = pq._filters_to_expression(filter)
        else:
            filter_expression = None
        return (
            pa_ds.dataset(
                source=f,
                schema=schema,
                filesystem=self.fs,
                format="parquet",
                partitioning="hive",
            )
            .to_table(filter=filter_expression, columns=self.columns)
            .to_pandas()
        )

    def _make_meta_from_schema(self):
        meta = dict()

        for field in self.schema:
            if self.columns:
                if field.name in self.columns:
                    meta[field.name] = field.type.to_pandas_dtype()
            else:
                meta[field.name] = field.type.to_pandas_dtype()
        return meta

    def _history_helper(self, log_file_name):
        log = self.fs.cat(log_file_name).decode().split("\n")
        for line in log:
            if line:
                meta_data = json.loads(line)
                if "commitInfo" in meta_data:
                    return meta_data["commitInfo"]

    def history(self, limit=None, **kwargs):
        delta_log_path = str(self.path).rstrip("/") + "/_delta_log"
        log_files = self.fs.glob(f"{delta_log_path}/*.json")
        if len(log_files) == 0:  # pragma no cover
            raise RuntimeError(f"No History (logs) found at:- {delta_log_path}/")
        log_files = sorted(log_files, reverse=True)
        if limit is None:
            last_n_files = log_files
        else:
            last_n_files = log_files[:limit]
        parts = [
            delayed(
                self._history_helper,
                name="read-delta-history" + tokenize(self.fs_token, f, **kwargs),
            )(f, **kwargs)
            for f in list(last_n_files)
        ]
        return dask.compute(parts)[0]

    def _vacuum_helper(self, filename_to_delete):
        full_path = urlparse(self.path)
        if full_path.scheme and full_path.netloc:  # pragma no cover
            # for different storage backend, delta-rs vacuum gives path to the file
            # it will not provide bucket name and scheme s3 or gcfs etc. so adding
            # manually
            filename_to_delete = (
                f"{full_path.scheme}://{full_path.netloc}/{filename_to_delete}"
            )
        self.fs.rm_file(filename_to_delete)

    def vacuum(self, retention_hours=168, dry_run=True):
        """
        Run the Vacuum command on the Delta Table: list and delete files no
        longer referenced by the Delta table and are older than the
        retention threshold.

        retention_hours: the retention threshold in hours, if none then
        the value from `configuration.deletedFileRetentionDuration` is used
         or default of 1 week otherwise.
        dry_run: when activated, list only the files, delete otherwise

        Returns
        -------
        the list of files no longer referenced by the Delta Table and are
         older than the retention threshold.
        """

        tombstones = self.dt.vacuum(retention_hours=retention_hours)
        if dry_run:
            return tombstones
        else:
            parts = [
                delayed(
                    self._vacuum_helper,
                    name="delta-vacuum"
                    + tokenize(self.fs_token, f, retention_hours, dry_run),
                )(f)
                for f in tombstones
            ]
        dask.compute(parts)[0]

    def get_pq_files(self):
        """
        get the list of parquet files after loading the
        current datetime version
        """
        __doc__ == self.dt.load_with_datetime.__doc__

        if self.datetime is not None:
            self.dt.load_with_datetime(self.datetime)
        return self.dt.file_uris()

    def read_delta_table(self, **kwargs):
        """
        Reads the list of parquet files in parallel
        """
        pq_files = self.get_pq_files()
        if len(pq_files) == 0:
            raise RuntimeError("No Parquet files are available")
        parts = [
            delayed(
                self.read_delta_dataset,
                name="read-delta-table-" + tokenize(self.fs_token, f, **kwargs),
            )(f, **kwargs)
            for f in list(pq_files)
        ]
        meta = self._make_meta_from_schema()
        return from_delayed(parts, meta=meta)


def read_delta_table(
    path, version=None, columns=None, storage_options=None, datetime=None, **kwargs,
):
    """
    Read a Delta Table into a Dask DataFrame

    This reads a list of Parquet files in delta table directory into a
    Dask.dataframe.

    Parameters
    ----------
    path: str
        path of Delta table directory
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
        Key/value pairs to be passed on to the file-system backend, if any.
    kwargs: dict,optional
        Some most used parameters can be passed here are:
        1. schema
        2. filter

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

    Returns
    -------
    Dask.DataFrame

    Examples
    --------
    >>> df = dd.read_delta_table('s3://bucket/my-delta-table')  # doctest: +SKIP

    """
    dtw = DeltaTableWrapper(
        path=path,
        version=version,
        columns=columns,
        storage_options=storage_options,
        datetime=datetime,
    )
    return dtw.read_delta_table(columns=columns, **kwargs)


def read_delta_history(path, limit=None, storage_options=None):
    """
    Run the history command on the DeltaTable.
    The operations are returned in reverse chronological order.

    Parallely reads delta log json files using dask delayed and gathers the
    list of commit_info (history)

    Parameters
    ----------
    path: str
        path of Delta table directory
    limit: int, default None
        the commit info limit to return, defaults to return all history

    Returns
    -------
        list of the commit infos registered in the transaction log
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.history(limit=limit)


def vacuum(path, retention_hours=168, dry_run=True, storage_options=None):
    """
    Run the Vacuum command on the Delta Table: list and delete
    files no longer referenced by the Delta table and are
    older than the retention threshold.

    retention_hours: int, default 168
    the retention threshold in hours, if none then the value
    from `configuration.deletedFileRetentionDuration` is used
    or default of 1 week otherwise.
    dry_run: bool, default True
        when activated, list only the files, delete otherwise

    Returns
    -------
    None or List of tombstones
    i.e the list of files no longer referenced by the Delta Table
    and are older than the retention threshold.
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.vacuum(retention_hours=retention_hours, dry_run=dry_run)
