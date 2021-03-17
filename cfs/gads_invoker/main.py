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
import sys
from typing import Any, Dict

from flask import Response
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
# from google.ads.google_ads.services import UploadClickConversionsResponse
from google.cloud import pubsub_v1
from google.cloud import storage


def _is_partial_failure_error_present(conversion_upload_response) -> bool:
  """Checks whether a response message has a partial failure error.

  Args:
      conversion_upload_response:  A conversion_upload_response instance.
  Returns:
      A boolean, whether or not the response message has a partial
      failure error.
  """
  partial_failure = getattr(conversion_upload_response, 'partial_failure_error',
                            None)
  code = getattr(partial_failure, 'code', None)

  return code != 0


def _count_partial_errors(client: GoogleAdsClient,
                          conversion_upload_response) -> int:
  """Counts the partial errors in the GAds response.

  Args:
      client: A GoogleAdsClient instance
      conversion_upload_response: Google Upload Conversion service response.

  Returns:
      An integer representing the total number of partial errors in the response
      failure error.
  """
  error_count = 0

  if _is_partial_failure_error_present(conversion_upload_response):
    partial_failure = getattr(conversion_upload_response,
                              'partial_failure_error', None)
    error_details = getattr(partial_failure, 'details', [])

    for error_detail in error_details:
      failure_message = client.get_type('GoogleAdsFailure', version='v6')
      failure_object = failure_message.FromString(error_detail.value)
      error_count += len(failure_object.errors)

  return error_count


def _add_errors_to_input_data(client: GoogleAdsClient,
                              conversion_upload_response,
                              data: Dict[str, Any]) -> Dict[str, Any]:
  """Includes the error count to the input data.

  Args:
    client: A GoogleAdsClient instance
    conversion_upload_response: Response from the conversions upload API call
    data: the input data received in the trigger invocation

  Returns:
    The input data enriched with the num errors
  """
  data['child']['num_errors'] = _count_partial_errors(
      client, conversion_upload_response)
  return data


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


def _upload_conversions(config, project_id, reporting_topic, client,
                        customer_id, conversions):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    config: Configuration information
    project_id (string): ID of the Google Cloud Project
    reporting_topic (string): Pub/Sub topic for reporting messages
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
        'ConversionUploadService', version='v6')
    conversions_list.append(click_conversion)

  try:
    # Upload the conversions using the API and handle any possible errors
    print('Proceeding to upload the conversions')
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
        customer_id, conversions_list, partial_failure=True)
    print('Conversions upload process successfully completed')
    pubsub_payload = _add_errors_to_input_data(client,
                                               conversion_upload_response,
                                               config)
    publisher = pubsub_v1.PublisherClient()
    topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
    publisher.publish(topic_path_reporting, data=bytes(pubsub_payload, 'utf-8'))
    print('Pub/Sub message sent')
  except GoogleAdsException as ex:
    print(f'Request with ID "{ex.request_id}" failed with status '
          f'"{ex.error.code().name}" and includes the following errors:')
    for error in ex.failure.errors:
      print(f'\tError with message "{error.message}".')
      if error.location:
        for field_path_element in error.location.field_path_elements:
          print(f'\t\tOn field: {field_path_element.field_name}')
    return 200
  except:
    print('Unexpected error:', sys.exc_info()[0])
    pubsub_payload = _add_errors_to_input_data(client,
                                               conversion_upload_response,
                                               config)
    publisher = pubsub_v1.PublisherClient()
    topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
    publisher.publish(topic_path_reporting, data=bytes(pubsub_payload, 'utf-8'))
    print('Pub/Sub message sent')
    return 500


def _gads_invoker_worker(client, bucket_name, config, project_id,
                         reporting_topic):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    client: Initialized instance of a Google Ads API client
    bucket_name (string): Name of the bucket to read the chunk from
    config (string): Configuration information.
    project_id (string): ID of the Google Cloud Project
    reporting_topic (string): Pub/Sub topic for reporting messages

  Returns:
    None
  """
  datestamp = config['date']
  customer_id = config['parent']['cid']
  chunk_filename = config['child']['file_name']
  # Load filename from GCS
  # Load chunk file
  conversions_blob = _read_csv_from_blob(
      bucket_name, datestamp + '/slices_processing/' + chunk_filename)
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  result = _upload_conversions(config, project_id, reporting_topic, client,
                               customer_id, conversions_list)
  return result


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
  credentials = {
      'developer_token': developer_token,
      'client_id': client_id,
      'client_secret': client_secret,
      'refresh_token': refresh_token,
      'login_customer_id': login_customer_id
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
        `make_response
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  if not all(elem in os.environ for elem in [
      'DEFAULT_GCS_BUCKET', 'CLIENT_ID', 'DEVELOPER_TOKEN', 'CLIENT_SECRET',
      'REFRESH_TOKEN', 'LOGIN_CUSTOMER_ID', 'DEFAULT_GCP_PROJECT',
      'STORE_RESPONSE_STATS_TOPIC'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)

  bucket_name = os.environ['DEFAULT_GCS_BUCKET']
  client_id = os.environ['CLIENT_ID']
  developer_token = os.environ['DEVELOPER_TOKEN']
  client_secret = os.environ['CLIENT_SECRET']
  refresh_token = os.environ['REFRESH_TOKEN']
  login_customer_id = os.environ['LOGIN_CUSTOMER_ID']
  project_id = os.environ['DEFAULT_GCP_PROJECT']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  request_json = request.get_json(silent=True)
  if request_json and 'parent' in request_json and 'child' in request_json:
    client = _initialize_gads_client(client_id, developer_token, client_secret,
                                     refresh_token, login_customer_id,
                                     request_json)
    result = _gads_invoker_worker(client, bucket_name, request_json, project_id,
                                  reporting_topic)
    return Response('', result)
  else:
    print('ERROR: Configuration not found, please POST it in your request')
    return Response('', 400)
