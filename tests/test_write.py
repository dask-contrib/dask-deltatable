from __future__ import annotations

import os
import unittest.mock as mock

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from dask.datasets import timeseries

from dask_deltatable import read_deltalake
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
@pytest.mark.parametrize("freq,partition_freq", [("1H", "1D"), ("1H", "1w")])
def test_roundtrip(tmpdir, with_index, freq, partition_freq):
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
        freq=freq,
        partition_freq=partition_freq,
        dtypes=dtypes,
    )

    ddf = ddf.reset_index()
    if with_index:
        ddf = ddf.set_index("timestamp")

    out = to_deltalake(tmpdir, ddf, compute=False)
    assert not os.listdir(tmpdir)
    out.compute()
    assert len(os.listdir(tmpdir)) > 0

    ddf_read = read_deltalake(tmpdir)
    ddf_dask = dd.read_parquet(tmpdir)

    assert ddf.npartitions == ddf_read.npartitions
    # By default, arrow reads with ns resolution
    assert_eq(ddf_read, ddf_dask)


@mock.patch("dask_deltatable.utils.maybe_set_aws_credentials")
def test_writer_check_aws_credentials(maybe_set_aws_credentials, tmpdir):
    # The full functionality of maybe_set_aws_credentials tests in test_utils
    # we only need to ensure it's called here when writing with a str path
    maybe_set_aws_credentials.return_value = dict()

    df = pd.DataFrame({"col1": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)
    to_deltalake(str(tmpdir), ddf)
    maybe_set_aws_credentials.assert_called()


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_datetime(tmpdir, unit):
    """Ensure we can write datetime with different resolutions,
    at least one-way only"""
    tmpdir = str(tmpdir)
    ts = pd.date_range("2023-01-01", periods=10, freq="1D", unit=unit)
    df = pd.DataFrame({"ts": pd.Series(ts)})
    ddf = dd.from_pandas(df, npartitions=2)
    to_deltalake(tmpdir, ddf)
    ddf_read = read_deltalake(tmpdir)
    ddf_dask = dd.read_parquet(tmpdir)
    assert_eq(ddf_read, ddf_dask, check_index=False)
