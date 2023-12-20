import pytest
from unittest.mock import patch, MagicMock
from dags.api_request import get_meta, get_breweries, paginate_api, upload_json_to_s3
import requests
import os
from datetime import datetime


def test_endpoint_get_meta():
    expected = "https://api.openbrewerydb.org/v1/breweries/meta"
    actual = get_meta().url
    assert actual == expected


@pytest.mark.parametrize("per_page, page", [(3, 1), (200, 10), (50, 1)])
def test_endpoint_get_breweries(per_page, page):
    payload = {"per_page": per_page, "page": page}
    expected = (
        f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
    )
    actual = get_breweries(payload).url
    assert actual == expected


@patch("dags.api_request.requests.get")
def test_get_meta_with_error(mock_requests_get):
    # simulate an exception when calling requestes.get
    mock_requests_get.side_effect = requests.exceptions.RequestException(
        "Simulated error"
    )
    actual = get_meta()
    assert actual is None


@patch("dags.api_request.requests.get")
def test_get_meta_success(mock_requests_get):
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_requests_get.return_value = mock_response
    actual = get_meta()
    assert actual.status_code == 200


@patch("dags.api_request.get_breweries")
def test_paginate_api_returns_list(mock_get_breweries):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_get_breweries.return_value = mock_response

    actual = paginate_api(100, 10)
    assert isinstance(actual, list)


@patch("dags.api_request.boto3.client")
@patch("dags.api_request.logger.info")
def test_upload_json_to_s3(mock_logger_info, mock_boto3_client):
    current_date = datetime.now().date()
    with patch.dict(
        "os.environ",
        {
            "AWS_ACCESS_KEY_ID": "FAKE_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "FAKE_SECRET_ACCESS_KEY",
        },
    ):

        # Configuring the simulated return for put_object method
        mock_boto3_client.return_value.put_object.return_value = (
            (
                f"raw/extracted_at={current_date}/list-breweries_{datetime.now().date()}.json"
            )
            .replace(" ", "_")
            .replace(":", "-")
        )

        # Passing test values
        json_string = '{"key": "value"}'
        bucket_name = "bucket-name"
        upload_json_to_s3(json_string, bucket_name)

        # check if put_object is ok
        mock_boto3_client.return_value.put_object.assert_called_once_with(
            Body = json_string,
            Bucket = bucket_name,
            Key = mock_boto3_client.return_value.put_object.return_value,
        )
