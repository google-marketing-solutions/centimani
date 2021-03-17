"""Google Cloud function that uploads the chunk of conversions to Google Ads."""

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-

import csv
import io
import os
import random
import string
import sys
from typing import Sequence

from absl import app
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.cloud import storage


def _read_csv_from_blob(bucket_name, blob_name):
  """Function to read a blob containing a CSV file and return it as an array.

  Args:
    bucket_name (string): name of the source bucket
    blob_name (string): name of the file to move
  Returns:
    Decoded blob with the contents of the file
  """
  storage_client = storage.Client()
  print('Reading {} from {}'.format(blob_name, bucket_name))
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  downloaded_blob = blob.download_as_string()
  decoded_blob = downloaded_blob.decode('utf-8')
  return decoded_blob


def _gads_invoker_worker(client, bucket_name, config):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    client: Initialized instance of a Google Ads API client
    bucket_name (string): Name of the bucket to read the chunk from
    config (string): Configuration information.
  Returns:
    None
  """
  datestamp = config['date']
  customer_id = config['parent']['cid']
  chunk_filename = config['child']['file_name']
  # Load filename from GCS
  # Load chunk file
  conversions_blob = _read_csv_from_blob(bucket_name, datestamp + '/slices_processing/' + chunk_filename)
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  _upload_conversions(client, customer_id, conversions_list)


def _upload_conversions(client, customer_id, conversions):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    client: Initialized instance of a Google Ads API client
    customer_id (string): CID of parent account to associate the upload with.
    conversions (list): Chunk of conversions containing details about each one.
  Returns:
    None
  """
  # Create a list of click conversions to be uploaded
  conversions_list = []
  for conversion_info in conversions:
    click_conversion = client.get_type('ClickConversion', version='v6')
    click_conversion.gclid = conversion_info[0]
    click_conversion.conversion_date_time = conversion_info[2]
    click_conversion.conversion_value = float(conversion_info[3])
    click_conversion.currency_code = conversion_info[4]

    conversion_upload_service = client.get_service(
        'ConversionUploadService', version='v6'
    )
    conversions_list.append(click_conversion)

  try:
    # Upload the conversions using the API and handle any possible errors
    print('Proceeding to upload the conversions')
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
        customer_id, conversions_list, partial_failure=True)
    # print('API response: {}'.format(conversion_upload_response))
  except GoogleAdsException as ex:
    print(
        f'Request with ID "{ex.request_id}" failed with status '
        f'"{ex.error.code().name}" and includes the following errors:'
    )
    for error in ex.failure.errors:
      print(f'\tError with message "{error.message}".')
      if error.location:
        for field_path_element in error.location.field_path_elements:
          print(f'\t\tOn field: {field_path_element.field_name}')
    sys.exit(1)


def _initialize_gads_client(client_id, developer_token, client_secret,
                            refresh_token, login_customer_id, config):
  """Initialized and returns Google Ads API client.

  Args:
    client_id (string): Client ID used by the API client
    developer_token (string): Developer Token used by the API client
    client_secret (string): Client Secret used by the API client
    refresh_token(string): Refresh Token used by the API client
    login_customer_id(string): Customer ID used for login
    config(string): Configuration settings passed to the Cloud Function
  Returns:
    None
  """
  credentials =  {
    'developer_token': developer_token,
    'client_id': client_id,
    'client_secret': client_secret,
    'refresh_token': refresh_token,
    'login_customer_id': '8767308703'
  }
  client = GoogleAdsClient.load_from_dict(credentials)
  return client


def gads_invoker(request):
  """Triggers the upload of a chunk of conversions.

  Args:
    request (flask.Request): HTTP request object.
  Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  if not all(elem in os.environ for elem in [
      'DEFAULT_GCS_BUCKET', 'CLIENT_ID', 'DEVELOPER_TOKEN', 'CLIENT_SECRET',
      'REFRESH_TOKEN', 'LOGIN_CUSTOMER_ID'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)
  bucket_name = os.environ['DEFAULT_GCS_BUCKET']
  client_id = os.environ['CLIENT_ID']
  developer_token = os.environ['DEVELOPER_TOKEN']
  client_secret = os.environ['CLIENT_SECRET']
  refresh_token = os.environ['REFRESH_TOKEN']
  login_customer_id  = os.environ['LOGIN_CUSTOMER_ID']
  request_json = request.get_json(silent=True)
  if request_json and 'parent' in request_json and 'child' in request_json:
    client = _initialize_gads_client(client_id, developer_token, client_secret,
                                     refresh_token, login_customer_id,
                                     request_json)
    _gads_invoker_worker(client, bucket_name, request_json)
  else:
    print('ERROR: Configuration not found, please POST it in your request')

