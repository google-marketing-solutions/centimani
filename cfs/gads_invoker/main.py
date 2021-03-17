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
import datetime
import io
import json
import os
import pytz
import sys
from typing import Any, Dict, Sequence

from absl import app
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
                              data: Dict[str, Any],
                              num_errors: int) -> Dict[str, Any]:
  """Includes the error count to the input data.

  Args:
    client: A GoogleAdsClient instance
    data: the input data received in the trigger invocation
    num_errors: the number of errors to add

  Returns:
    The input data enriched with the num errors
  """
  data['child']['num_errors'] = num_errors
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


def _upload_conversions(input_json, project_id, reporting_topic, client,
                        customer_id, conversions):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    input_json: Configuration information
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
    conversion_action_service = client.get_service(
        "ConversionActionService", version="v6"
    )
    click_conversion.conversion_action = conversion_action_service.conversion_action_path(
     customer_id, '340032850'
    )

    click_conversion.gclid = conversion_info[0]
    date_time_obj = datetime.datetime.strptime(conversion_info[2], '%Y-%m-%d %H:%M:%S')
    timezone = pytz.timezone('Europe/Madrid')
    timezone_date_time_obj = timezone.localize(date_time_obj)
    adjusted_date_str = timezone_date_time_obj.strftime('%Y-%m-%d %H:%M:%S%z')
    click_conversion.conversion_date_time = adjusted_date_str[:-2] + ':' + adjusted_date_str[-2:]
    click_conversion.conversion_value = float(conversion_info[3])
    click_conversion.currency_code = conversion_info[4]
    conversions_list.append(click_conversion)

  try:
    conversion_upload_service = client.get_service(
        'ConversionUploadService', version='v6')
    # Upload the conversions using the API and handle any possible errors
    print('Proceeding to upload conversions for customer_id {}'.format(customer_id))
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
        customer_id, conversions_list, partial_failure=True)
    print(conversion_upload_response)
    pubsub_payload = _add_errors_to_input_data(client,
                                               input_json,
                        _count_partial_errors(client, conversion_upload_response)
                        )
    publisher = pubsub_v1.PublisherClient()
    topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
    publisher.publish(topic_path_reporting, data=bytes(json.dumps(pubsub_payload), 'utf-8')).result()
    print('Pub/Sub message sent')
    return 200
  except GoogleAdsException as ex:
    print(f'Request with ID "{ex.request_id}" failed with status '
          f'"{ex.error.code().name}" and includes the following errors:')
    for error in ex.failure.errors:
      print(f'\tError with message "{error.message}".')
      if error.location:
        for field_path_element in error.location.field_path_elements:
          print(f'\t\tOn field: {field_path_element.field_name}')
    pubsub_payload = _add_errors_to_input_data(
                        client,
                        input_json,
                        input_json['child']['num_rows'])
    publisher = pubsub_v1.PublisherClient()
    topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
    publisher.publish(topic_path_reporting, data=bytes(json.dumps(pubsub_payload), 'utf-8')).result()
    print('Pub/Sub message sent')
    return 200
  except:
    print('Unexpected error:', sys.exc_info()[0])
    pubsub_payload = _add_errors_to_input_data(
                        client,
                        input_json,
                        input_json['child']['num_rows'])
    publisher = pubsub_v1.PublisherClient()
    topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
    publisher.publish(topic_path_reporting, data=bytes(json.dumps(pubsub_payload), 'utf-8')).result()
    print('Pub/Sub message sent')
    return 200


def _gads_invoker_worker(client, bucket_name, input_json, project_id,
                         reporting_topic):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    client: Initialized instance of a Google Ads API client
    bucket_name (string): Name of the bucket to read the chunk from
    input_json (string): Configuration information.
    project_id (string): ID of the Google Cloud Project
    reporting_topic (string): Pub/Sub topic for reporting messages

  Returns:
    None
  """
  datestamp = input_json['date']
  customer_id = input_json['parent']['cid']
  chunk_filename = input_json['child']['file_name']
  # Load filename from GCS
  # Load chunk file
  conversions_blob = _read_csv_from_blob(
      bucket_name, datestamp + '/slices_processing/' + chunk_filename)
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  result = _upload_conversions(input_json, project_id, reporting_topic, client,
                               customer_id, conversions_list)
  return result


def _initialize_gads_client(client_id, developer_token, client_secret,
                            refresh_token, login_customer_id, input_json):
  """Initialized and returns Google Ads API client.

  Args:
    client_id (string): Client ID used by the API client
    developer_token (string): Developer Token used by the API client
    client_secret (string): Client Secret used by the API client
    refresh_token(string): Refresh Token used by the API client
    login_customer_id(string): Customer ID used for login
    input_json(string): Configuration settings passed to the Cloud Function

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
      'STORE_RESPONSE_STATS_TOPIC', 'DEPLOYMENT_NAME', 'SOLUTION_PREFIX'
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
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'

  input_json = request.get_json(silent=True)
  if input_json and 'parent' in input_json and 'child' in input_json:
    client = _initialize_gads_client(client_id, developer_token, client_secret,
                                     refresh_token, login_customer_id,
                                     input_json)
    result = _gads_invoker_worker(client, bucket_name, input_json, project_id,
                                  full_path_topic)
    return Response('', result)
  else:
    print('ERROR: Configuration not found, please POST it in your request')
    return Response('', 400)


def main(argv: Sequence[str]) -> None:
  """Main function for testing using the command line.

  Args:
      argv (typing.Sequence): argument list
  Returns:
      None
  """
  # Replace with your testing JSON
  input_string = '{"date": "20210316", "target_platform": "GAds", "parent": {"cid": "3533563242", "file_name": "EGO_313-4134-123_20210101_test.csv", "file_path": "input", "file_date": "20210101", "total_files": 13, "total_rows": 25000}, "child": {"file_name": "EGO_313-4134-123_20210101_test.csv---2", "num_rows": 2000}}'
  input_json = json.loads(input_string)

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
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'

  client = _initialize_gads_client(client_id, developer_token, client_secret,
                                   refresh_token, login_customer_id,
                                   input_json)
  result = _gads_invoker_worker(client, bucket_name, input_json, project_id,
                                full_path_topic)
  print('Test execution returned: {}'.format(result))

if __name__ == '__main__':
  app.run(main)
