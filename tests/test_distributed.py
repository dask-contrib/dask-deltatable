from __future__ import annotations

import pytest

distributed = pytest.importorskip("distributed")

import os  # noqa: E402
import sys  # noqa: E402

import pyarrow as pa  # noqa: E402
import pyarrow.dataset as pa_ds  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from dask.datasets import timeseries  # noqa: E402
from distributed.utils_test import cleanup  # noqa F401
from distributed.utils_test import (  # noqa F401
    client,
    cluster,
    cluster_fixture,
    gen_cluster,
    loop,
    loop_in_thread,
    popen,
    varying,
)

import dask_deltatable as ddt  # noqa: E402

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "The teardown of distributed.utils_test.cluster_fixture "
        "fails on windows CI currently"
    ),
)


def test_write(client, tmpdir):
    ddf = timeseries(
        start="2023-01-01",
        end="2023-01-03",
        freq="1H",
        partition_freq="1D",
        dtypes={"str": object, "float": float, "int": int},
    ).reset_index()
    ddt.to_deltalake(f"{tmpdir}", ddf)


def test_write_with_options(client, tmpdir):
    file_options = dict(compression="gzip")
    ddf = timeseries(
        start="2023-01-01",
        end="2023-01-03",
        freq="1H",
        partition_freq="1D",
        dtypes={"str": object, "float": float, "int": int},
    ).reset_index()
    ddt.to_deltalake(f"{tmpdir}", ddf, file_options=file_options)
    parquet_filename = [f for f in os.listdir(tmpdir) if f.endswith(".parquet")][0]
    parquet_file = pq.ParquetFile(f"{tmpdir}/{parquet_filename}")
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"


def test_write_with_schema(client, tmpdir):
    ddf = timeseries(
        start="2023-01-01",
        end="2023-01-03",
        freq="1H",
        partition_freq="1D",
        dtypes={"str": object, "float": float, "int": int},
    ).reset_index()
    schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us")),
            pa.field("str", pa.string()),
            pa.field("float", pa.float32()),
            pa.field("int", pa.int32()),
        ]
    )
    ddt.to_deltalake(f"{tmpdir}", ddf, schema=schema)
    ds = pa_ds.dataset(str(tmpdir))
    assert ds.schema == schema


def test_read(client, simple_table):
    df = ddt.read_deltalake(simple_table)
    assert df.columns.tolist() == ["id", "count", "temperature", "newColumn"]
    assert df.compute().shape == (200, 4)
