from __future__ import annotations

import pytest

from dask_deltatable.utils import get_partition_filters


@pytest.mark.parametrize(
    "cols,filters,expected",
    [
        [[], None, None],
        [[], [("part", ">", "a")], None],
        [["part"], [("part", ">", "a"), ("x", "==", 1)], [("part", ">", "a")]],
        [["part"], [[("part", ">", "a")], [("x", "==", 1)]], [("part", ">", "a")]],
        [
            ["m", "d"],
            [("m", ">", 5), ("d", "=", 1), ("x", "==", "a")],
            [("m", ">", 5), ("d", "=", 1)],
        ],
        [
            ["m", "d"],
            [[("m", ">", 5)], [("d", "=", 1)], [("x", "==", "a")]],
            [("m", ">", 5), ("d", "=", 1)],
        ],
        [["x"], [("x", ">", 5), ("x", ">", 6)], [("x", ">", 5)]],
        [["x"], [("x", "<", 5), ("x", "<", 6)], [("x", "<", 6)]],
        [["x"], [("x", "=", 5), ("x", "=", 6)], [("x", "in", [5, 6])]],
        [["x"], [("x", "!=", 5), ("x", "!=", 6)], [("x", "not in", [5, 6])]],
        [["x"], [("x", "!=", 5), ("y", "!=", 6)], [("x", "!=", 5)]],
    ],
)
def test_partition_filters(cols, filters, expected):
    res = get_partition_filters(cols, filters)
    assert res == expected
    if isinstance(filters, list):
        # make sure it works with additional level of wrapping
        res = get_partition_filters(cols, filters)
        assert res == expected
