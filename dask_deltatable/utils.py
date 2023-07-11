from __future__ import annotations

from typing import cast

from .types import Filter, Filters


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
