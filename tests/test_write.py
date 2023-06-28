from __future__ import annotations

import os

import pytest
from dask.dataframe.utils import assert_eq
from dask.datasets import timeseries

from dask_deltatable import read_delta_table
from dask_deltatable.write import to_deltalake


@pytest.mark.parametrize(
    "with_index",
    [
        pytest.param(
            True,
            marks=[
                pytest.mark.xfail(
                    reason="TS index is always ns resolution but delta can only handle us"
                )
            ],
        ),
        False,
    ],
)
def test_roundtrip(tmpdir, with_index):
    dtypes = {
        "str": object,
        # FIXME: Categorical data does not work
        # "category": "category",
        "float": float,
        "int": int,
    }
    tmpdir = str(tmpdir)
    ddf = timeseries(
        start="2023-01-01",
        end="2023-01-15",
        # FIXME: Setting the partition frequency destroys the roundtrip for some
        # reason
        # partition_freq="1w",
        dtypes=dtypes,
    )
    # FIXME: us is the only precision delta supports. This lib should likely
    # case this itself

    ddf = ddf.reset_index()
    ddf.timestamp = ddf.timestamp.astype("datetime64[us]")
    if with_index:
        ddf = ddf.set_index("timestamp")

    out = to_deltalake(tmpdir, ddf)
    assert not os.listdir(tmpdir)
    out.compute()
    assert len(os.listdir(tmpdir)) > 0

    ddf_read = read_delta_table(tmpdir)
    # FIXME: The index is not recovered
    if with_index:
        ddf = ddf.reset_index()

    # By default, arrow reads with ns resolution
    ddf.timestamp = ddf.timestamp.astype("datetime64[ns]")
    assert_eq(ddf, ddf_read)
