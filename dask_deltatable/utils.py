from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import date, datetime
from decimal import Decimal
from math import inf
from typing import Any, cast
from urllib.parse import unquote

from deltalake import DeltaTable

from .types import Filter, Filters


def get_bucket_region(path: str):
    import boto3

    if not path.startswith("s3://"):
        raise ValueError(f"'{path}' is not an S3 path")
    bucket = path.replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    # Buckets in region 'us-east-1' results in None, b/c why not.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#S3.Client.get_bucket_location
    return resp["LocationConstraint"] or "us-east-1"


def maybe_set_aws_credentials(path: Any, options: dict[str, Any]) -> dict[str, Any]:
    """
    Maybe set AWS credentials into ``options`` if existing AWS specific keys
    not found in it and path is s3:// format.

    Parameters
    ----------
    path : Any
        If it's a string, we'll check if it starts with 's3://' then determine bucket
        region if the AWS credentials should be set.
    options : dict[str, Any]
        Options, any kwargs to be supplied to things like S3FileSystem or similar
        that may accept AWS credentials set. A copy is made and returned if modified.

    Returns
    -------
    dict
        Either the original options if not modified, or a copied and updated options
        with AWS credentials inserted.
    """

    is_s3_path = getattr(path, "startswith", lambda _: False)("s3://")
    if not is_s3_path:
        return options

    # Avoid overwriting already provided credentials
    keys = ("AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "access_key", "secret_key")
    if not any(k in (options or ()) for k in keys):
        # defers installing boto3 upfront, xref _read_from_catalog
        import boto3

        session = boto3.session.Session()
        credentials = session.get_credentials()
        if credentials is None:
            return options
        region = get_bucket_region(path)

        options = (options or {}).copy()
        options.update(
            # Capitalized is used in delta specific API and lowercase is for S3FileSystem
            dict(
                # TODO: w/o this, we need to configure a LockClient which seems to require dynamodb.
                AWS_S3_ALLOW_UNSAFE_RENAME="true",
                AWS_SECRET_ACCESS_KEY=credentials.secret_key,
                AWS_ACCESS_KEY_ID=credentials.access_key,
                AWS_SESSION_TOKEN=credentials.token,
                AWS_REGION=region,
                secret_key=credentials.secret_key,
                access_key=credentials.access_key,
                token=credentials.token,
                region=region,
            )
        )
    return options


def get_partition_filters(
    partition_columns: list[str], filters: Filters
) -> list[list[Filter]] | None:
    """Retrieve only filters on partition columns. If there are any row filters in the outer
    list (the OR list), return None, because we have to search through all partitions to apply
    row filters

    Parameters
    ----------
    partition_columns : List[str]
        List of partitioned columns

    filters : List[Tuple[str, str, Any]] | List[List[Tuple[str, str, Any]]]
        List of filters. Examples:
        1) (x == a) and (y == 3):
           [("x", "==", "a"), ("y", "==", 3)]
        2) (x == a) or (y == 3)
            [[("x", "==", "a")], [("y", "==", 3)]]

    Returns
    -------
    List[List[Tuple[str, str, Any]]] | None
        List of partition filters, None if we can't apply a filter on partitions because
        row filters are present
    """
    if filters is None or len(filters) == 0:
        return None

    if isinstance(filters[0][0], str):
        filters = cast(list[list[Filter]], [filters])
    filters = cast(list[list[Filter]], filters)

    allowed_ops = {
        "=": "=",
        "==": "=",
        "!=": "!=",
        "!==": "!=",
        "in": "in",
        "not in": "not in",
        ">": ">",
        "<": "<",
        ">=": ">=",
        "<=": "<=",
    }

    expressions = []
    for disjunction in filters:
        inner_expressions = []
        for col, op, val in disjunction:
            if col in partition_columns:
                normalized_op = allowed_ops[op]
                inner_expressions.append((col, normalized_op, val))
        if inner_expressions:
            expressions.append(inner_expressions)
        else:
            return None

    return expressions if expressions else None


# Copied from delta-rs v0.25.5 (https://github.com/delta-io/delta-rs/blob/python-v0.25.5/LICENSE.txt)
def get_partitions_from_path(path: str) -> tuple[str, dict[str, str | None]]:
    if path[0] == "/":
        path = path[1:]
    parts = path.split("/")
    parts.pop()  # remove filename
    out: dict[str, str | None] = {}
    for part in parts:
        if part == "":
            continue
        key, value = part.split("=", maxsplit=1)
        if value == "__HIVE_DEFAULT_PARTITION__":
            out[key] = None
        else:
            out[key] = unquote(value)
    return path, out


# Copied from delta-rs v0.25.5 (https://github.com/delta-io/delta-rs/blob/python-v0.25.5/LICENSE.txt)
def get_file_stats_from_metadata(
    metadata: Any,
    num_indexed_cols: int,
    columns_to_collect_stats: list[str] | None,
) -> dict[str, int | dict[str, Any]]:
    """Get Delta's file stats from PyArrow's Parquet file metadata."""
    stats = {
        "numRecords": metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }

    def iter_groups(metadata: Any) -> Iterator[Any]:
        for i in range(metadata.num_row_groups):
            if metadata.row_group(i).num_rows > 0:
                yield metadata.row_group(i)

    schema_columns = metadata.schema.names
    if columns_to_collect_stats is not None:
        idx_to_iterate = []
        for col in columns_to_collect_stats:
            try:
                idx_to_iterate.append(schema_columns.index(col))
            except ValueError:
                pass
    elif num_indexed_cols == -1:
        idx_to_iterate = list(range(metadata.num_columns))
    elif num_indexed_cols >= 0:
        idx_to_iterate = list(range(min(num_indexed_cols, metadata.num_columns)))
    else:
        raise ValueError("delta.dataSkippingNumIndexedCols valid values are >=-1")

    for column_idx in idx_to_iterate:
        name = metadata.row_group(0).column(column_idx).path_in_schema

        # If stats missing, then we can't know aggregate stats
        if all(
            group.column(column_idx).is_stats_set for group in iter_groups(metadata)
        ):
            stats["nullCount"][name] = sum(
                group.column(column_idx).statistics.null_count
                for group in iter_groups(metadata)
            )

            # Min / max may not exist for some column types, or if all values are null
            if any(
                group.column(column_idx).statistics.has_min_max
                for group in iter_groups(metadata)
            ):
                # Min and Max are recorded in physical type, not logical type
                # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                # TODO: Add logic to decode physical type for DATE, DECIMAL

                minimums = (
                    group.column(column_idx).statistics.min
                    for group in iter_groups(metadata)
                )
                # If some row groups have all null values, their min and max will be null too.
                min_value = min(minimum for minimum in minimums if minimum is not None)
                # Infinity cannot be serialized to JSON, so we skip it. Saying
                # min/max is infinity is equivalent to saying it is null, anyways.
                if min_value != -inf:
                    stats["minValues"][name] = min_value
                maximums = (
                    group.column(column_idx).statistics.max
                    for group in iter_groups(metadata)
                )
                max_value = max(maximum for maximum in maximums if maximum is not None)
                if max_value != inf:
                    stats["maxValues"][name] = max_value
    return stats


# Copied from delta-rs v0.25.5 (https://github.com/delta-io/delta-rs/blob/python-v0.25.5/LICENSE.txt)
class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


# Inspired from delta-rs v0.25.5 (https://github.com/delta-io/delta-rs/blob/python-v0.25.5/LICENSE.txt)
def get_num_idx_cols_and_stats_columns(
    table: DeltaTable | None, configuration: Mapping[str, str | None] | None
) -> tuple[int, list[str] | None]:
    """Get the num_idx_columns and stats_columns from the table configuration in the state

    If table does not exist (only can occur in the first write action) it takes
    the configuration that was passed.
    """
    if table is not None:
        configuration = table.metadata().configuration
    if configuration is None:
        num_idx_cols = -1
        stats_columns = None
    else:
        # Parse configuration
        dataSkippingNumIndexedCols = configuration.get(
            "delta.dataSkippingNumIndexedCols", "-1"
        )
        num_idx_cols = (
            int(dataSkippingNumIndexedCols)
            if dataSkippingNumIndexedCols is not None
            else -1
        )
        columns = configuration.get("delta.dataSkippingStatsColumns", None)
        if columns is not None:
            stats_columns = [col.strip() for col in columns.split(",")]
        else:
            stats_columns = None
    return num_idx_cols, stats_columns
