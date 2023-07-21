from __future__ import annotations

import os
import zipfile

import pytest

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "tests", "data")


@pytest.fixture()
def simple_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/simple.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/test1/"


@pytest.fixture()
def simple_table2(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/simple2.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/simple_table/"


@pytest.fixture()
def partition_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/partition.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/test2/"


@pytest.fixture()
def empty_table1(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/empty1.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty/"


@pytest.fixture()
def empty_table2(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/empty2.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty2/"


@pytest.fixture()
def checkpoint_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile(f"{DATA_DIR}/checkpoint.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/checkpoint/"
