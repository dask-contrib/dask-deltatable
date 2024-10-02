from __future__ import annotations

from typing import Any, cast

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
