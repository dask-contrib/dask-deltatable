from __future__ import annotations

import pathlib
import unittest.mock as mock

import pytest

from dask_deltatable.utils import (
    get_bucket_region,
    get_partition_filters,
    maybe_set_aws_credentials,
)


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


@mock.patch("dask_deltatable.utils.get_bucket_region")
@pytest.mark.parametrize(
    "options",
    (
        None,
        dict(),
        dict(AWS_ACCESS_KEY_ID="foo", AWS_SECRET_ACCESS_KEY="bar"),
        dict(access_key="foo", secret_key="bar"),
    ),
)
@pytest.mark.parametrize("path", ("s3://path", "/another/path", pathlib.Path(".")))
def test_maybe_set_aws_credentials(
    mocked_get_bucket_region,
    options,
    path,
):
    pytest.importorskip("boto3")

    mocked_get_bucket_region.return_value = "foo-region"

    mock_creds = mock.MagicMock()
    type(mock_creds).token = mock.PropertyMock(return_value="token")
    type(mock_creds).access_key = mock.PropertyMock(return_value="access-key")
    type(mock_creds).secret_key = mock.PropertyMock(return_value="secret-key")

    def mock_get_credentials():
        return mock_creds

    with mock.patch("boto3.session.Session") as mocked_session:
        session = mocked_session.return_value
        session.get_credentials.side_effect = mock_get_credentials

        opts = maybe_set_aws_credentials(path, options)

    if options and not any(k in options for k in ("AWS_ACCESS_KEY_ID", "access_key")):
        assert opts["AWS_ACCESS_KEY_ID"] == "access-key"
        assert opts["AWS_SECRET_ACCESS_KEY"] == "secret-key"
        assert opts["AWS_SESSION_TOKEN"] == "token"
        assert opts["AWS_REGION"] == "foo-region"

        assert opts["access_key"] == "access-key"
        assert opts["secret_key"] == "secret-key"
        assert opts["token"] == "token"
        assert opts["region"] == "foo-region"

    # Did not alter input options if credentials were supplied by user
    elif options:
        assert options == opts


@pytest.mark.parametrize("location", (None, "region-foo"))
@pytest.mark.parametrize(
    "path,bucket",
    (("s3://foo/bar", "foo"), ("s3://fizzbuzz", "fizzbuzz"), ("/not/s3", None)),
)
def test_get_bucket_region(location, path, bucket):
    pytest.importorskip("boto3")

    with mock.patch("boto3.client") as mock_client:
        mock_client = mock_client.return_value
        mock_client.get_bucket_location.return_value = {"LocationConstraint": location}

        if not path.startswith("s3://"):
            with pytest.raises(ValueError, match="is not an S3 path"):
                get_bucket_region(path)
            return

        region = get_bucket_region(path)

    # AWS returns None if bucket located in us-east-1...
    location = location if location else "us-east-1"
    assert region == location

    mock_client.get_bucket_location.assert_has_calls([mock.call(Bucket=bucket)])
