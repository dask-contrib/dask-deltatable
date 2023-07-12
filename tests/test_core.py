from __future__ import annotations

import glob
import os
import zipfile
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable

import dask_deltatable as ddt


@pytest.fixture()
def simple_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/simple.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/test1/"


@pytest.fixture()
def simple_table2(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/simple2.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/simple_table/"


@pytest.fixture()
def partition_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/partition.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/test2/"


@pytest.fixture()
def empty_table1(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/empty1.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty/"


@pytest.fixture()
def empty_table2(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/empty2.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty2/"


@pytest.fixture()
def checkpoint_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/checkpoint.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/checkpoint/"


def test_read_delta(simple_table):
    df = ddt.read_deltalake(simple_table)

    assert df.columns.tolist() == ["id", "count", "temperature", "newColumn"]
    assert df.compute().shape == (200, 4)


def test_read_delta_with_different_versions(simple_table):
    print(simple_table)
    df = ddt.read_deltalake(simple_table, version=0)
    assert df.compute().shape == (100, 3)

    df = ddt.read_deltalake(simple_table, version=1)
    assert df.compute().shape == (200, 4)


def test_row_filter(simple_table):
    # row filter
    df = ddt.read_deltalake(
        simple_table,
        version=0,
        filter=[("count", ">", 30)],
    )
    assert df.compute().shape == (61, 3)


def test_different_columns(simple_table):
    df = ddt.read_deltalake(simple_table, columns=["count", "temperature"])
    assert df.columns.tolist() == ["count", "temperature"]


def test_different_schema(simple_table):
    # testing schema evolution

    df = ddt.read_deltalake(simple_table, version=0)
    assert df.columns.tolist() == ["id", "count", "temperature"]

    df = ddt.read_deltalake(simple_table, version=1)
    assert df.columns.tolist() == ["id", "count", "temperature", "newColumn"]


@pytest.mark.parametrize(
    "kwargs,shape",
    [
        (dict(version=0, filter=[("col1", "==", 1)]), (21, 3)),
        (dict(filter=[("col1", "==", 1), ("col2", "<", 0.5)]), (11, 4)),
        (dict(filter=[[("col1", "==", 1)], [("col1", "==", 2)]]), (39, 4)),
        (dict(filter=[("col1", "!=", 1), ("id", "<", 5)]), (6, 4)),
        (dict(filter=[[("col1", "!=", 1)], [("id", "<", 5)]]), (99, 4)),
    ],
)
def test_partition_filter(partition_table, kwargs, shape):
    """partition filter"""
    df = ddt.read_deltalake(partition_table, **kwargs)
    filter_expr = pq.filters_to_expression(kwargs["filter"])
    dt = DeltaTable(partition_table, version=kwargs.get("version"))
    expected_partitions = len(
        list(dt.to_pyarrow_dataset().get_fragments(filter=filter_expr))
    )
    assert df.npartitions == expected_partitions
    assert df.compute().shape == shape


def test_empty(empty_table1, empty_table2):
    df = ddt.read_deltalake(empty_table1, version=4)
    assert df.compute().shape == (0, 2)

    df = ddt.read_deltalake(empty_table1, version=0)
    assert df.compute().shape == (5, 2)

    with pytest.raises(RuntimeError):
        # No Parquet files found
        _ = ddt.read_deltalake(empty_table2)


def test_read_delta_table_warns(simple_table):
    with pytest.warns(DeprecationWarning, match="`read_delta_table` was renamed"):
        ddt.read_delta_table(simple_table)


def test_checkpoint(checkpoint_table):
    df = ddt.read_deltalake(checkpoint_table, checkpoint=0, version=4)
    assert df.compute().shape[0] == 25

    df = ddt.read_deltalake(checkpoint_table, checkpoint=10, version=12)
    assert df.compute().shape[0] == 65

    df = ddt.read_deltalake(checkpoint_table, checkpoint=20, version=22)
    assert df.compute().shape[0] == 115

    with pytest.raises(Exception):
        # Parquet file with the given checkpoint 30 does not exists:
        # File {checkpoint_path} not found"
        _ = ddt.read_deltalake(checkpoint_table, checkpoint=30, version=33)


def test_out_of_version_error(simple_table):
    # Cannot time travel Delta table to version 4 , Available versions for given
    # checkpoint 0 are [0,1]
    with pytest.raises(Exception):
        _ = ddt.read_deltalake(simple_table, version=4)


def test_load_with_datetime(simple_table2):
    log_dir = f"{simple_table2}_delta_log"
    log_mtime_pair = [
        ("00000000000000000000.json", 1588398451.0),
        ("00000000000000000001.json", 1588484851.0),
        ("00000000000000000002.json", 1588571251.0),
        ("00000000000000000003.json", 1588657651.0),
        ("00000000000000000004.json", 1588744051.0),
    ]
    for file_name, dt_epoch in log_mtime_pair:
        file_path = os.path.join(log_dir, file_name)
        os.utime(file_path, (dt_epoch, dt_epoch))

    expected = ddt.read_deltalake(simple_table2, version=0).compute()
    result = ddt.read_deltalake(
        simple_table2, datetime="2020-05-01T00:47:31-07:00"
    ).compute()
    assert expected.equals(result)
    # assert_frame_equal(expected,result)

    expected = ddt.read_deltalake(simple_table2, version=1).compute()
    result = ddt.read_deltalake(
        simple_table2, datetime="2020-05-02T22:47:31-07:00"
    ).compute()
    assert expected.equals(result)

    expected = ddt.read_deltalake(simple_table2, version=4).compute()
    result = ddt.read_deltalake(
        simple_table2, datetime="2020-05-25T22:47:31-07:00"
    ).compute()
    assert expected.equals(result)


def test_read_delta_with_error():
    with pytest.raises(ValueError) as exc_info:
        ddt.read_deltalake()
    assert str(exc_info.value) == "Please Provide Delta Table path"


def test_catalog_with_error():
    with pytest.raises(ValueError) as exc_info:
        ddt.read_deltalake(catalog="glue")
    assert (
        str(exc_info.value)
        == "Since Catalog was provided, please provide Database and table name"
    )


def test_catalog(simple_table):
    dt = MagicMock()

    def delta_mock(**kwargs):
        files = glob.glob(simple_table + "/*parquet")
        dt.file_uris = MagicMock(return_value=files)
        return dt

    with patch("deltalake.DeltaTable.from_data_catalog", side_effect=delta_mock):
        os.environ["AWS_ACCESS_KEY_ID"] = "apple"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "greatsecret"
        df = ddt.read_deltalake(
            catalog="glue", database_name="stores", table_name="orders"
        )
        assert df.compute().shape == (200, 3)
