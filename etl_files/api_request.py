import logging
import json
import time
import math
import requests
import requests_cache
import boto3
import datetime
from datetime import datetime
import os
from tempfile import NamedTemporaryFile, SpooledTemporaryFile
from botocore.exceptions import NoCredentialsError
from IPython.display import clear_output

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
requests_cache.install_cache()

def get_meta():
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    try:
        return requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        return None


def get_breweries(payload):
    url = "https://api.openbrewerydb.org/v1/breweries"
    try:
        return requests.get(url, params=payload)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        return None


def paginate_api(total, per_page):
    total_pages = math.ceil(total / per_page)
    responses = []

    for page in range(1, total_pages + 1):
        payload = {"per_page": per_page, "page": page}

        logger.info(f"Requesting page {page}/{total_pages}")
        clear_output(wait=True)

        response = get_breweries(payload)
        response.raise_for_status()
        responses.append(response)

        if not getattr(response, "from_cache", False):
            time.sleep(0.40)

        # time.sleep(0.40)

    return responses


def get_responses():
    logger.info("Starting the data retrieval process.")

    meta_response = get_meta()
    total = int(meta_response.json()["total"])
    per_page = int(meta_response.json()["per_page"])

    responses = paginate_api(total, per_page)

    logger.info("Data retrieval process completed successfully.")

    return responses


def write_json(data, filename):
    with open(filename, "w") as f:
        json.dump([r.json() for r in data], f, indent=2)
    logger.info(f"Local file '{filename}' created successfully.")


def convert_responses_to_flat_list(responses):
    flat_list = [item for response in responses for item in response.json()]
    return json.dumps(flat_list, indent=2)


def upload_json_to_s3(json_string, bucket_name):
    s3 = boto3.client("s3")
    object_key = (
        (
            f"raw/extracted_at={datetime.now().date()}/list-breweries_{datetime.now()}.json"
        )
        .replace(" ", "_")
        .replace(":", "-")
    )

    s3.put_object(Body=json_string, Bucket=bucket_name, Key=object_key)
    logger.info(f"JSON data uploaded to S3 bucket: {bucket_name}/{object_key}")


responses = get_responses()
s3_bucket_name = "open-brewerie-db"
json_string = convert_responses_to_flat_list(responses)
upload_json_to_s3(json_string, s3_bucket_name)
