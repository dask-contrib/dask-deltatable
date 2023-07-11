from __future__ import annotations

import pytest

from dask_deltatable.utils import get_partition_filters


@pytest.mark.parametrize(
    "cols,filters,expected",
    [
        [[], None, None],
        [[], [("part", ">", "a")], None],
        [["part"], [("part", ">", "a"), ("x", "==", 1)], [[("part", ">", "a")]]],
        [["part"], [[("part", ">", "a")], [("x", "==", 1)]], None],
        [
            ["m", "d"],
            [("m", ">", 5), ("d", "=", 1), ("x", "==", "a")],
            [[("m", ">", 5), ("d", "=", 1)]],
        ],
        [
            ["m", "d"],
            [[("m", ">", 5)], [("d", "=", 1)], [("x", "==", "a")]],
            None,
        ],
    ],
)
def test_partition_filters(cols, filters, expected):
    res = get_partition_filters(cols, filters)
    assert res == expected
    if isinstance(filters, list):
        # make sure it works with additional level of wrapping
        res = get_partition_filters(cols, filters)
        assert res == expected
