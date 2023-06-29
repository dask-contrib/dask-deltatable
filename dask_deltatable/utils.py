from __future__ import annotations

from typing import Any


def get_partition_filters(partition_columns, filters):
    """Retrieve only filters on partition columns

    Parameters
    ----------
    partition_columns : list[str]
        List of partitioned columns

    filters : list[tuple[str, str, Any]]
        List of filters. Example: [("x", "==", "a")]

    Returns
    -------
    result : Optional[list[tuple[str, str, Any]]]
        List of filters, without row filters
    """
    if not filters:
        return None

    if isinstance(filters[0][0], str):
        filters = [filters]

    allowed_ops = ["=", "!=", "in", "not in", ">", "<", ">=", "<="]
    partition_filters: dict[str, dict[str, list[Any]]] = {
        col: {op: [] for op in allowed_ops} for col in partition_columns
    }
    for conjunctions in filters:
        for tpl in conjunctions:
            if isinstance(tpl, tuple) and tpl[0] in partition_columns:
                col, op, val = tpl
                if op in ("=", "=="):
                    partition_filters[col]["in"].append(val)
                elif op in ("!=", "!=="):
                    partition_filters[col]["not in"].append(val)
                elif op in allowed_ops:
                    partition_filters[col][op].append(val)

    def compress_opval(col, op, val):
        if op == "in" and len(val) == 1:
            return col, "=", val[0]
        if op == "not in" and len(val) == 1:
            return col, "!=", val[0]
        if op in ("<", "<="):
            return col, op, max(val)
        if op in (">", ">="):
            return col, op, min(val)
        return col, op, val

    compressed_filters = [
        compress_opval(col, op, val)
        for col, ops in partition_filters.items()
        for op, val in ops.items()
        if len(val) > 0
    ]
    return compressed_filters if compressed_filters else None
