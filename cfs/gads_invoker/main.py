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
import logging
import os
import sys
# import traceback
from typing import Any, Dict

from flask import Response
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import datastore as store
from google.cloud import pubsub_v1
from google.cloud import secretmanager
from google.cloud import storage
import pytz


def _upsert_conversion_actions_in_cache(db: store.client.Client,
                                        customer_id: str,
                                        actions: Dict[str, Any],
                                        cache_ttl_in_hours: int):
  """Inserts the cid and the conversion actions belonging to it into the cache.

  Args:
      db: Datastore client
      customer_id: the cid of the customer
      actions: dictionary with the data to store
      cache_ttl_in_hours: the time to live of the cache
  """
  key = db.key('cid', customer_id)
  now = datetime.datetime.now(pytz.utc)
  delta = datetime.timedelta(hours=cache_ttl_in_hours)
  actions['expiration_timestamp'] = now + delta

  with db.transaction():
    cid = store.Entity(key=key)
    cid.update({'actions': actions})
    db.put(cid)


def _get_conversion_actions_from_cache(db: store.client.Client,
                                       customer_id: str) -> Dict[str, Any]:
  """Retrieves the resources for all conversion actions "indexed" by name.

  In order to overcome GAds API quota problemas, it firsts looks up in the
  cache (datastore) for the value, if not hit it will query GAds API.

  Args:
      db: Datastore client
      customer_id: the cid of the customer

  Returns:
      A dictionary with all the results
  """

  key = db.key('cid', customer_id)

  with db.transaction():
    res = db.get(key)

  if res:
    dict_result = res['actions']
    if dict_result and dict_result[
        'expiration_timestamp'] < datetime.datetime.now(pytz.utc):
      return None
    else:
      del dict_result['expiration_timestamp']
      return dict_result
  else:
    return None


def _get_conversion_action_resources(client: GoogleAdsClient,
                                     customer_id: str,
                                     cache_ttl_in_hours: int) -> Dict[str, Any]:
  """Retrieves the resources for all conversion actions "indexed" by name.

  In order to overcome GAds API quota problemas, it firsts looks up in the
  cache (datastore) for the value, if not hit it will query GAds API.

  Args:
      client: GADsClient
      customer_id: the cid of the customer
      cache_ttl_in_hours: the time to live of the cache

  Returns:
      A dictionary with all the results
  """

  db = store.Client()
  cached_result = _get_conversion_actions_from_cache(db, customer_id)
  if cached_result:
    return cached_result
  else:
    ga_service = client.get_service('GoogleAdsService')
    print('Extracting conversion information from customer ID {}'.format(
        customer_id))
    dict_result = {}
    query = """
        SELECT conversion_action.id, conversion_action.name FROM conversion_action
        """
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
      for row in batch.results:
        dict_result[
            row.conversion_action.name] = row.conversion_action.resource_name
    _upsert_conversion_actions_in_cache(db, customer_id, dict_result,
                                        cache_ttl_in_hours)
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
      A list containing the code, message and number of times that each unique
      error code was returned by the API for one of the conversions uploaded.
  """
  error_count = 0
  error_stats = {}
  error_array = []
  if _is_partial_failure_error_present(conversion_upload_response):
    partial_failure = getattr(conversion_upload_response,
                              'partial_failure_error', None)
    error_details = getattr(partial_failure, 'details', [])

    for error_detail in error_details:
      failure_message = client.get_type('GoogleAdsFailure')
      google_ads_failure = type(failure_message)
      failure_object_des = google_ads_failure.deserialize(error_detail.value)
      error_count += len(failure_object_des.errors)
      for error in failure_object_des.errors:
        str_code = str(error.error_code).strip()
        if str_code in error_stats:
          error_stats[str_code]['count'] += 1
        else:
          error_stats[str_code] = {}
          error_stats[str_code]['count'] = 1
          error_stats[str_code]['message'] = str(error.message).strip()
        print('A partial failure at index '
              f'{error.location.field_path_elements[0].index} occurred '
              f'\nError message: {error.message}\nError code: '
              f'{error.error_code}')
    for code_key in error_stats:
      error_array.append({
          'code': code_key,
          'message': error_stats[code_key]['message'],
          'count': error_stats[code_key]['count']
      })
  return error_count, error_array


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


def _mv_blob_if_last_try(task_retries, max_attempts, input_json, bucket_name):
  """Checks if it is the last attempt and moves the chunk to the failed folder.

  Args:
    task_retries: Retry number passed from Cloud Tasks
    max_attempts: Max number of configured retries
    input_json: Configuration information
    bucket_name: Name of the GCS file storing the chunks

  Returns:
    None
  """

  if task_retries + 1 == max_attempts:
    datestamp = input_json['date']
    chunk_filename = input_json['child']['file_name']
    full_chunk_path = datestamp + '/slices_processing/' + chunk_filename
    new_file_name = full_chunk_path.replace('slices_processing/',
                                            'slices_failed/')
    _mv_blob(bucket_name, full_chunk_path, bucket_name, new_file_name)


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

  # Set timezone, extracting it from extra_parameters if supplied
  timezone = pytz.timezone('Etc/GMT')
  if 'extra_parameters' in input_json and input_json['extra_parameters']:
    custom_csv_params = input_json['extra_parameters']
    timezone = pytz.timezone('Etc/GMT')
    for csv_param_info in custom_csv_params:
      if 'TimeZone' in csv_param_info:
        timezone = pytz.timezone(csv_param_info.split('=')[1])

  print('Time zone set to {}'.format(timezone))

  # Create a list of click conversions to be uploaded
  conversions_list = []
  for conversion_info in conversions:
    click_conversion = client.get_type('ClickConversion')
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
    if conversion_info[3].strip():
      click_conversion.conversion_value = float(conversion_info[3])
    else:
      click_conversion.conversion_value = 0
    click_conversion.currency_code = conversion_info[4]
    conversions_list.append(click_conversion)

  try:
    conversion_upload_service = client.get_service(
        'ConversionUploadService')
    # Upload the conversions using the API and handle any possible errors
    num_conversions = len(conversions_list)
    print(
        f'Proceeding to upload {num_conversions} conversions for customer_id {customer_id}'
    )
    request = client.get_type('UploadClickConversionsRequest')
    request.customer_id = customer_id
    request.conversions = conversions_list
    request.partial_failure = True
    conversion_upload_response = (
        conversion_upload_service.upload_click_conversions(
            request=request,
        )
    )
    num_partial_errors, error_array = _count_partial_errors(
        client, conversion_upload_response)
    pubsub_payload = _add_errors_to_input_data(
        input_json, num_partial_errors)
    if error_array:
      pubsub_payload['child']['errors'] = error_array
    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # Move blob to /slices_processed after a successful execution
    new_file_name = full_chunk_path.replace('slices_processing/',
                                            'slices_processed/')
    _mv_blob(bucket_name, full_chunk_path, bucket_name, new_file_name)
    return 200
  except GoogleAdsException:
    pubsub_payload = _add_errors_to_input_data(input_json,
                                               input_json['child']['num_rows'])
    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # If last try, move blob to /slices_failed
    _mv_blob_if_last_try(task_retries, max_attempts, input_json, bucket_name)
    return 500
  except Exception:
    print('Unexpected error:', sys.exc_info()[0])
    pubsub_payload = _add_errors_to_input_data(input_json,
                                               input_json['child']['num_rows'])
    _send_pubsub_message(project_id, reporting_topic, pubsub_payload)
    # If last try, move blob to /slices_failed
    _mv_blob_if_last_try(task_retries, max_attempts, input_json, bucket_name)
    return 500


def _send_pubsub_message(project_id, reporting_topic, pubsub_payload):
  """Checks if a blob exists in Google Cloud Storage.

  Args:
    project_id: ID of the Google Cloud Project where the solution is deployed.
    reporting_topic: Pub/Sub topic to use in the message to be sent.
    pubsub_payload: Payload of the Pub/Sub message to be sent.
  Returns:
    None
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path_reporting = publisher.topic_path(project_id, reporting_topic)
  publisher.publish(
      topic_path_reporting, data=bytes(json.dumps(pubsub_payload),
                                       'utf-8')).result()


def _blob_exists(bucket_name, blob_name):
  """Checks if a blob exists in Google Cloud Storage.

  Args:
    bucket_name: Name of the bucket to read the blob from
    blob_name: Name of the blob to check
  Returns:
    Boolean indicating if the blob exists or not
  """

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  return storage.Blob(bucket=bucket, name=blob_name).exists(storage_client)


def _check_conversions_blob(datestamp, bucket_name, chunk_filename):
  """Checks if a blob exists either in the processing or processed directories.

  Args:
    datestamp: timestamp used to build the directory name
    bucket_name: Name of the bucket to read the blob from
    chunk_filename: Name of the chunk to check
  Returns:
    Full path to the blob or None if it cannot be found
  """
  processing_chunk_name = datestamp + '/slices_processing/' + chunk_filename
  if _blob_exists(bucket_name, processing_chunk_name):
    print('Found conversions blob in slices_processing folder')
    return processing_chunk_name
  else:
    processed_chunk_name = datestamp + '/slices_processed/' + chunk_filename
    print('Looking for {}'.format(processing_chunk_name))
    if _blob_exists(bucket_name, processed_chunk_name):
      print('Found conversions blob in slices_processed folder')
      return processed_chunk_name
    else:
      print('ERROR: Blob not found')
      return None


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
  full_chunk_name = _check_conversions_blob(datestamp, bucket_name,
                                            chunk_filename)
  if full_chunk_name:
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
  else:
    input_json['child']['num_errors'] = input_json['child']['num_rows']
    _send_pubsub_message(project_id, reporting_topic, input_json)
    return 200


def _initialize_gads_client(config: Dict[str, Any],
                            login_customer_id: str) -> GoogleAdsClient:
  """Initialized and returns Google Ads API client.

  Args:
    config: a dictionary with the platform configuration
    login_customer_id: the customer id to act as

  Returns:
    GoogleAdsClient
  Raises:
    Exception: Credentials for login_customer_id not found.
  """
  logging.basicConfig(
      level=logging.WARNING,
      format='[%(asctime)s - %(levelname)s] %(message).5000s')
  logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)
  if login_customer_id in config['credentials']:
    client = GoogleAdsClient.load_from_dict(
        config['credentials'][login_customer_id])
    return client
  else:
    raise Exception(f'Credentials for {login_customer_id} not found')


def _get_max_attempts(config: Dict[str, Any]) -> int:
  """Retrieves the max attempts from the config.

  Args:
    config: a dictionary with the platform configuration

  Returns:
    The number of attempts
  """
  return config['queue_config']['retry_config']['max_attempts']


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
  Raises:
    Exception: File name format is not correct.
  """
  arr = input_json['parent']['file_name'].split('_')

  if len(arr) < 4:
    raise Exception('File name format is not correct')

  return (arr[2].replace('-', ''), arr[3].replace('-',
                                                  ''), arr[4].replace('-', ''))


def _read_platform_config_from_secret(project_id: str,
                                      secret_id: str) -> Dict[str, Any]:
  """Gets the config for the platform.

  Args:
      project_id: the project name where the secret is defined
      secret_id: string representing the id of the secret with the config

  Returns:
      A dictionary containing the conifguration
  """
  # Create the Secret Manager client.
  client = secretmanager.SecretManagerServiceClient()
  # Build the resource name of the secret version.
  name = f'projects/{project_id}/secrets/{secret_id}/versions/latest'
  # Access the secret version.
  response = client.access_secret_version(request={'name': name})
  payload = response.payload.data.decode('UTF-8')
  data = json.loads(payload)

  return data


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
  required_elem = [
      'OUTPUT_GCS_BUCKET', 'PROJECT_ID',
      'STORE_RESPONSE_STATS_TOPIC', 'DEPLOYMENT_NAME', 'SOLUTION_PREFIX',
      'CACHE_TTL_IN_HOURS'
  ]
  if not all(elem in os.environ for elem in required_elem):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)

  bucket_name = os.environ['OUTPUT_GCS_BUCKET']
  project_id = os.environ['PROJECT_ID']
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  cache_ttl_in_hours = int(os.environ['CACHE_TTL_IN_HOURS'])
  config = _read_platform_config_from_secret(
      project_id, f'{deployment_name}_{solution_prefix}_gads_config')
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'
  input_json = request.get_json(silent=True)

  task_retries = -1
  try:
    max_attempts = int(_get_max_attempts(config))
    if 'X-Cloudtasks-Taskretrycount' in request.headers:
      task_retries = int(request.headers.get('X-Cloudtasks-Taskretrycount'))
      print('Got {} task retries from Cloud Tasks'.format(task_retries))
    (_, login_cid, conversions_holder_cid) = _extract_cids(input_json)

    client = _initialize_gads_client(config, login_cid)
    conversions_resources = _get_conversion_action_resources(
        client, conversions_holder_cid, cache_ttl_in_hours)
    result = _gads_invoker_worker(client, bucket_name, input_json,
                                  conversions_resources, project_id,
                                  full_path_topic, task_retries, max_attempts)

    return Response('', result)
  except Exception:
    print('ERROR: Unexpected exception raised during the process: ',
          sys.exc_info()[0])
    # str_traceback = traceback.format_exc()
    print('Unexpected exception traceback follows:')
    # print(str_traceback)

    pubsub_payload = _add_errors_to_input_data(input_json,
                                               input_json['child']['num_rows'])
    _send_pubsub_message(project_id, full_path_topic, pubsub_payload)
    # If last try, move blob to /slices_failed
    _mv_blob_if_last_try(task_retries, max_attempts, input_json, bucket_name)
    return Response('', 500)


def  _test_main() -> None:
  """Main function for testing using the command line.

  Args:
    None

  Returns:
    None
  """
  # Replace with your testing JSON
  input_string = (' {"date": "YYYYMMDD", '
    '"target_platform": "gads",'
    '"extra_parameters": ["Parameters:TimeZone=Europe/Madrid", "", "", "", ""],'
    '"parent": {"cid": "XXXXXXXXXX", "file_name": "GADS_XX_AAA-BBB-CCCC_DDD-EEE-FFFF_GGG-HHH-III_YYYYMMDD_1.csv",'
    '"file_path": "input",'
    '"file_date": "YYYYMMDD",'
    '"total_files": 100,'
    '"total_rows": 25000},'
    '"child": {"file_name": "GADS_XX_AAA-BBB-CCCC_DDD-EEE-FFFF_GGG-HHH-III_YYYYMMDD_1.csv---3",'
    '"num_rows": 250}}')

  input_json = json.loads(input_string)

  if not all(elem in os.environ for elem in [
      'OUTPUT_GCS_BUCKET', 'PROJECT_ID',
      'STORE_RESPONSE_STATS_TOPIC', 'CACHE_TTL_IN_HOURS'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)

  bucket_name = os.environ['OUTPUT_GCS_BUCKET']
  project_id = os.environ['PROJECT_ID']
  deployment_name = os.environ['DEPLOYMENT_NAME']
  solution_prefix = os.environ['SOLUTION_PREFIX']
  cache_ttl_in_hours = int(os.environ['CACHE_TTL_IN_HOURS'])
  config = _read_platform_config_from_secret(
      project_id, f'{deployment_name}_{solution_prefix}_gads_config')
  reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
  max_attempts = _get_max_attempts(config)
  full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'
  task_retries = -1

  try:
    (_, login_cid, conversions_holder_cid) = _extract_cids(input_json)

    client = _initialize_gads_client(config, login_cid)

    conversions_resources = _get_conversion_action_resources(
        client, conversions_holder_cid, cache_ttl_in_hours)
    result = _gads_invoker_worker(client, bucket_name, input_json,
                                  conversions_resources, project_id,
                                  full_path_topic, task_retries, max_attempts)
    print('Test execution returned: {}'.format(result))
  except Exception:
    print('Unexpected exception raised during the process')
    raise

if __name__ == '__main__':
  _test_main()
