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
from google.cloud import pubsub_v1
from google.cloud import storage


def _get_conversion_action_resources(client: GoogleAdsClient,
                                     customer_id: str) -> Dict[str, Any]:
  """Retrieves the resources for all conversion actions "indexed" by name.

  Args:
      client: GADsClient
      customer_id: the cid of the customer

  Returns:
      A boolean, whether or not the response message has a partial
      failure error.
  """
  ga_service = client.get_service('GoogleAdsService', version='v6')
  print('Extracting conversion information from customer ID {}'.format(
      customer_id))
  dict_result = {}
  query = """
      SELECT conversion_action.id, conversion_action.name FROM conversion_action
      """
  response = ga_service.search_stream(customer_id, query)
  # response = ga_service.search_stream('1147121970', query)# customer_id, query)
  for batch in response:
    for row in batch.results:
      dict_result[
          row.conversion_action.name] = row.conversion_action.resource_name

  return dict_result


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


def _add_errors_to_input_data(data: Dict[str, Any],
                              num_errors: int) -> Dict[str, Any]:
  """Includes the error count to the input data.

  Args:
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


def _mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
  """Function for moving files between directories or buckets in GCP.

  Args:
    bucket_name (string): name of the source bucket
    blob_name (string): name of the file to move
    new_bucket_name (string): name of target bucket (can be same as original)
    new_blob_name (string): name of file in target bucket

  Returns:
      None
  """
  storage_client = storage.Client()
  source_bucket = storage_client.get_bucket(bucket_name)
  source_blob = source_bucket.blob(blob_name)
  destination_bucket = storage_client.get_bucket(new_bucket_name)

  # copy to new destination
  source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
  # delete from source
  source_blob.delete()

  print(f'File moved from {blob_name} to {new_blob_name}')


def _upload_conversions(input_json: Dict[str, Any],
                        conversion_actions_resources: Dict[str, Any],
                        project_id: str, reporting_topic: str,
                        client: GoogleAdsClient, customer_id: str, conversions,
                        task_retries: int, max_attempts: int, bucket_name: str,
                        full_chunk_path: str):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    input_json: Configuration information
    conversion_actions_resources: the info about the conversions definition
    project_id: ID of the Google Cloud Project
    reporting_topic: Pub/Sub topic for reporting messages
    client: Initialized instance of a Google Ads API client
    customer_id: CID of parent account to associate the upload with.
    conversions: Chunk of conversions containing details about each one.
    task_retries: Number of retries performed in Cloud Tasks
    max_attempts: Max number of retries configured in Cloud Tasks
    bucket_name: Name of the GCS file storing the chunks
    full_chunk_path: Full path to the chunk being processed

  Returns:
    None
  """

  # conversion_actions_resources = _get_conversion_action_resources(
  #    client, input_json['parent']['cid'])

  # Set timezone, extracting it from extra_parameters if supplied
  timezone = pytz.timezone('Etc/GMT')
  if 'extra_parameters' in input_json and input_json['extra_parameters']:
    custom_csv_params = input_json['extra_parameters']
    timezone = pytz.timezone('Etc/GMT')
    for csv_param_info in custom_csv_params:
      if 'TimeZone' in csv_param_info:
        equal_sign_pos = csv_param_info.find('=')
        timezone = csv_param_info[-1 * equal_sign_pos]
  print('Time zone set to {}'.format(timezone))

  # Create a list of click conversions to be uploaded
  conversions_list = []
  for conversion_info in conversions:
    click_conversion = client.get_type('ClickConversion', version='v6')
    click_conversion.gclid = conversion_info[0]
    click_conversion.conversion_action = conversion_actions_resources[
        conversion_info[1]]

    date_time_obj = datetime.datetime.strptime(conversion_info[2],
                                               '%Y-%m-%d %H:%M:%S')
    date_time_obj = date_time_obj + datetime.timedelta(seconds=60)
    timezone_date_time_obj = timezone.localize(date_time_obj)
    adjusted_date_str = timezone_date_time_obj.strftime('%Y-%m-%d %H:%M:%S%z')
    click_conversion.conversion_date_time = adjusted_date_str[:
                                                              -2] + ':' + adjusted_date_str[
                                                                  -2:]
    click_conversion.conversion_value = float(conversion_info[3])
    click_conversion.currency_code = conversion_info[4]
    conversions_list.append(click_conversion)

  try:
    conversion_upload_service = client.get_service(
        'ConversionUploadService', version='v6')
    # Upload the conversions using the API and handle any possible errors
    print(f'Proceeding to upload conversions for customer_id {customer_id}')
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
        customer_id, conversions_list, partial_failure=True)

    pubsub_payload = _add_errors_to_input_data(
        input_json, _count_partial_errors(client, conversion_upload_response))
    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # Move blob to /slices_processed after a successful execution
    new_file_name = full_chunk_path.replace('slices_processing/',
                                            'slices_processed/')
    _mv_blob(bucket_name, full_chunk_path, bucket_name, new_file_name)
    return 200
  except GoogleAdsException as ex:
    # _print_errors(ex)
    pubsub_payload = _add_errors_to_input_data(input_json,
                                               input_json['child']['num_rows'])
    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # If last try, move blob to /slices_failed
    if task_retries + 1 == max_attempts:
      new_file_name = full_chunk_path.replace('slices_processing/',
                                              'slices_failed/')
      _mv_blob(bucket_name, full_chunk_path, bucket_name, new_file_name)
    return 500
  except:
    print('Unexpected error:', sys.exc_info()[0])
    pubsub_payload = _add_errors_to_input_data(input_json,
                                               input_json['child']['num_rows'])

    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # If last try, move blob to /slices_failed
    if task_retries + 1 == max_attempts:
      new_file_name = full_chunk_path.replace('slices_processing/',
                                              'slices_failed/')
      _mv_blob(bucket_name, full_chunk_path, bucket_name, new_file_name)
    return 500


def _print_errors(ex):
  print(f'Request with ID "{ex.request_id}" failed with status '
        f'"{ex.error.code().name}" and includes the following errors:')
  for error in ex.failure.errors:
    print(f'\tError with message "{error.message}".')
    if error.location:
      for field_path_element in error.location.field_path_elements:
        print(f'\t\tOn field: {field_path_element.field_name}')


def _send_pubsub_message(project_id, reporting_topic, pubsub_payload):
  publisher = pubsub_v1.PublisherClient()
  topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
  publisher.publish(
      topic_path_reporting, data=bytes(json.dumps(pubsub_payload),
                                       'utf-8')).result()


def _gads_invoker_worker(client: GoogleAdsClient, bucket_name: str,
                         input_json: Dict[str, Any],
                         conversion_actions_resources: Dict[str, Any],
                         project_id: str, reporting_topic: str,
                         task_retries: int, max_attempts: int):
  """Loads a chunk of conversions from GCS and sends it to the Google Ads API.

  Args:
    client: Initialized instance of a Google Ads API client
    bucket_name: Name of the bucket to read the chunk from
    input_json: Configuration information.
    conversion_actions_resources: the object containing the info about the
      actions
    project_id: ID of the Google Cloud Project
    reporting_topic: Pub/Sub topic for reporting messages
    task_retries: Number of retries performed in Cloud Tasks
    max_attempts: Max number of retries configured in Cloud Tasks

  Returns:
    None
  """
  datestamp = input_json['date']
  customer_id = input_json['parent']['cid']
  chunk_filename = input_json['child']['file_name']
  # Load filename from GCS
  # Load chunk file
  full_chunk_name = datestamp + '/slices_processing/' + chunk_filename
  conversions_blob = _read_csv_from_blob(bucket_name, full_chunk_name)
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  # Skip header line
  next(conversions_list)
  result = _upload_conversions(
      input_json,
      conversion_actions_resources,
      project_id,
      reporting_topic,
      client,
      customer_id,
      conversions_list,
      task_retries,
      max_attempts,
      bucket_name,
      full_chunk_name,
  )
  return result


def _initialize_gads_client(login_customer_id: str) -> GoogleAdsClient:
  """Initialized and returns Google Ads API client.

  Args:
    login_customer_id: the customer id to act as

  Returns:
    GoogleAdsClient
  """

  with open('gads_credentials.json') as json_file:
    data = json.load(json_file)

  if data["credentials"][login_customer_id]:
    client = GoogleAdsClient.load_from_dict(data[login_customer_id])
    return client
  else:
    raise Exception(f'Credentias for {login_customer_id} not found')


def _extract_cids(input_json: Dict[str, Any]) -> (str, str, str):
  """Extracts the 3 relevant cids from the input data.

    File format is as follows:

    gads_<free-text-without-underscore>_<cid>_<login-cid>_<conv-definition-cid>

  Args:
    input_json: Dict containing the incoming input message payload.

  Returns:
        The cid for the account the conversions were extracted from.<cid>
        The cid of the customer to login as.<login-cid>
        The cid of the account where the conversions are
        defined.<conv-definiton-cid>

  """
  arr = input_json['parent']['file_name'].split('_')

  if len(arr) < 4:
    raise Exception('File name format is not correct')

  return (arr[1].replace('-', ''), arr[2].replace('-',
                                                  ''), arr[3].replace('-', ''))


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
      'STORE_RESPONSE_STATS_TOPIC', 'DEPLOYMENT_NAME', 'SOLUTION_PREFIX',
      'CT_QUEUE_MAX_ATTEMPTS'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)

  bucket_name = os.environ['DEFAULT_GCS_BUCKET']
  project_id = os.environ['DEFAULT_GCP_PROJECT']
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  max_attempts = os.environ['CT_QUEUE_MAX_ATTEMPTS']
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'

  input_json = request.get_json(silent=True)

  task_retries = -1
  if 'X-AppEngine-TaskRetryCount' in request.headers:
    task_retries = request.headers.get('X-AppEngine-TaskRetryCount')
    print('Got {} task retries from Cloud Tasks'.format(task_retries))

  (file_cid, conversions_holder_cid, login_cid) = _extract_cids(input_json)

  if input_json and 'parent' in input_json and 'child' in input_json:

    client = _initialize_gads_client(login_cid)
    conversions_resources = _get_conversion_action_resources(
        client, conversions_holder_cid)
    result = _gads_invoker_worker(client, bucket_name, input_json,
                                  conversions_resources, project_id,
                                  full_path_topic, task_retries, max_attempts)

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

  input_string = ('{"date": "20210318", "target_platform": "gads", "parent": '
                  '{"cid": "5035699692", "file_name": '
                  '"PR_121-558-1270_1.csv", "file_path": "input", '
                  '"file_date": "20210101", "total_files": 13, "total_rows": '
                  '25000}, "child": {"file_name": '
                  '"PR_121-558-1270_1.csv---1", "num_rows": 3}}')

  input_string = ('{"date": "20210318", "target_platform": "gads", "parent": '
                  '{"cid": "3533563242", "file_name": '
                  '"EGO_353-356-3242_5_test.csv", "file_path": "input", '
                  '"file_date": "20210101", "total_files": 13, "total_rows": '
                  '25000}, "child": {"file_name": '
                  '"EGO_353-356-3242_5_test.csv---1", "num_rows": 3}}')

  input_string = (
      '{"date": "20210318", "target_platform": "gads", "parent": {"cid": '
      '"6330877141", "file_name": '
      '"EG_633-087-7141_114-712-1970_114-712-1970_1_test.csv", "file_path": '
      '"input", "file_date": "20210101", "total_files": 13, "total_rows": '
      '25000}, "child": {"file_name": '
      '"EG_633-087-7141_114-712-1970_114-712-1970_1_test.csv---1", "num_rows":'
      ' 3}}'
  )

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
  project_id = os.environ['DEFAULT_GCP_PROJECT']
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  max_attempts = os.environ['CT_QUEUE_MAX_ATTEMPTS']
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'
  task_retries = -1

  (file_cid, conversions_holder_cid, login_cid) = _extract_cids(input_json)

  client = _initialize_gads_client(login_cid)
  conversions_resources = _get_conversion_action_resources(
      client, conversions_holder_cid)

  result = _gads_invoker_worker(client, bucket_name, input_json,
                                conversions_resources, project_id,
                                full_path_topic, task_retries, max_attempts)
  print('Test execution returned: {}'.format(result))


if __name__ == '__main__':
  app.run(main)
