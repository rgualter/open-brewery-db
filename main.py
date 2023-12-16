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
# Get metadata information from the API
def get_meta():
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    response = requests.get(url)
    return response


# Function to retrieve data from breweries API
def get_breweries(payload):
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url, params=payload)
    return response

# Paginate through the API
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

# Function to get API responses
def get_responses():
    logger.info("Starting the data retrieval process.")

    meta_response = get_meta()
    total = int(meta_response.json()["total"])
    per_page = int(meta_response.json()["per_page"])

    responses = paginate_api(total, per_page)

    logger.info("Data retrieval process completed successfully.")

    return responses


# Function to write data to local JSON file
def write_json(data, filename):
    with open(filename, "w") as f:
        json.dump([r.json() for r in data], f, indent=2)
    logger.info(f"Local file '{filename}' created successfully.")


def convert_responses_to_json(responses):
    """
    Convert a list of API responses to a JSON-formatted string.

    Parameters:
    - responses (list): List of API responses.

    Returns:
    - str: JSON-formatted string.
    """
    combined_json_data = [response.json() for response in responses]
    json_string = json.dumps(combined_json_data, indent=2)
    return json_string

def upload_json_to_s3(json_string, bucket_name):
    """
    Upload a JSON-formatted string to an S3 bucket.

    Parameters:
    - json_string (str): JSON-formatted string to be uploaded.
    - bucket_name (str): S3 bucket name.

    Returns:
    - None
    """
    s3 = boto3.client("s3")

    object_key = f"raw/extracted_at={datetime.now().date()}/list-breweries_{datetime.now()}.json"

    s3.put_object(Body=json_string, Bucket=bucket_name, Key=object_key)

    logger.info(f"JSON data uploaded to S3 bucket: {bucket_name}/{object_key}")


responses = get_responses()
s3_bucket_name = 'open-brewerie-db'
json_string = convert_responses_to_json(responses)
upload_json_to_s3(json_string, s3_bucket_name)
