"""Delta Acceptance Testing (DAT)

https://github.com/delta-incubator/dat

The DAT project provides test cases to verify different implementations of Delta Lake all behave
consistently. The expected behavior is described in the Delta Lake Protocol.

The tests cases are packaged into releases, which can be downloaded into CI jobs for automatic
testing. The test cases in this repo are represented using a standard file structure, so they
don't require any particular dependency or programming language.
"""

from __future__ import annotations

import os
import shutil
from urllib.request import urlretrieve

import dask.dataframe as dd
import pytest
from dask.dataframe.utils import assert_eq

import dask_deltatable as ddt

DATA_VERSION = "0.0.2"
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "out", "reader_tests", "generated")


@pytest.fixture(autouse=True, scope="session")
def download_data():
    """Download the data for the tests."""
    if not os.path.exists(DATA_DIR):
        filename = f"deltalake-dat-v{DATA_VERSION}.tar.gz"
        dest_filename = os.path.join(ROOT_DIR, filename)
        urlretrieve(
            f"https://github.com/delta-incubator/dat/releases/download/v{DATA_VERSION}/{filename}",
            dest_filename,
        )
        shutil.unpack_archive(dest_filename, ROOT_DIR)
        os.remove(dest_filename)
        assert os.path.exists(DATA_DIR)


def test_reader_all_primitive_types():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/all_primitive_types/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/all_primitive_types/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


@pytest.mark.parametrize("version,subdir", [(None, "latest"), (0, "v0"), (1, "v1")])
def test_reader_basic_append(version, subdir):
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/basic_append/delta", version=version)
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/basic_append/expected/{subdir}/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf, check_index=False)


@pytest.mark.parametrize("version,subdir", [(None, "latest"), (0, "v0"), (1, "v1")])
def test_reader_basic_partitioned(version, subdir):
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/basic_partitioned/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/basic_partitioned/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf, check_index=False)


@pytest.mark.xfail(reason="https://github.com/delta-io/delta-rs/issues/1533")
@pytest.mark.parametrize(
    "version,subdir", [(None, "latest"), (0, "v0"), (1, "v1"), (2, "v2")]
)
def test_reader_multi_partitioned(version, subdir):
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/multi_partitioned/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/multi_partitioned/expected/{subdir}/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf, check_index=False)


@pytest.mark.xfail(reason="https://github.com/delta-io/delta-rs/issues/1533")
def test_reader_multi_partitioned_2():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/multi_partitioned_2/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/multi_partitioned_2/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


def test_reader_nested_types():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/nested_types/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/nested_types/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


def test_reader_no_replay():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/no_replay/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/no_replay/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


def test_reader_no_stats():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/no_stats/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/no_stats/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


def test_reader_stats_as_structs():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/stats_as_struct/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/stats_as_struct/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


def test_reader_with_checkpoint():
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/with_checkpoint/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/with_checkpoint/expected/latest/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)


@pytest.mark.parametrize("version,subdir", [(None, "latest"), (1, "v1")])
def test_reader_with_schema_change(version, subdir):
    actual_ddf = ddt.read_deltalake(f"{DATA_DIR}/with_schema_change/delta")
    expected_ddf = dd.read_parquet(
        f"{DATA_DIR}/with_schema_change/expected/{subdir}/table_content/*parquet"
    )
    assert_eq(actual_ddf, expected_ddf)
