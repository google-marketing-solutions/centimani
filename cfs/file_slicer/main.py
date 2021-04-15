#!/usr/bin/python
"""Cloud function to split a CSV file uploaded to GCS into smaller chunks."""

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

import copy
import csv
import datetime
import io
import json
import math
import os
import random
import string
import sys
import hashlib

from typing import Any, Dict, Sequence, Optional
from absl import app
from google.cloud import storage
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
from google.cloud.functions_v1.context import Context
from google.protobuf import duration_pb2
from google.cloud import secretmanager
from google.cloud import pubsub_v1


def _send_pubsub_message(project_id, topic, pubsub_payload):
  """Sends a message to the pubsub topoc.

  Args:
    project_id: the id of the project where the topic exists
    topic: the name of the topic to send the message to
    pubsub_payload: the json payload to send to the topic

  """
  publisher = pubsub_v1.PublisherClient()
  topic_path_reporting = publisher.topic_path(project_id, topic)
  publisher.publish(
      topic_path_reporting, data=bytes(json.dumps(pubsub_payload),
                                       'utf-8')).result()

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


def _get_file_parameters(csv_line: str) -> Dict[str, Any]:
  if 'Parameters' in csv_line[0]:
    return csv_line
  else:
    return []


def _upsert_queue(client: tasks_v2.CloudTasksClient, queue_config: Dict[str,
                                                                        Any]):

  # Check if the queue exists
  try:
    queue = client.get_queue(name=queue_config['name'])
  except NotFound:
    queue = None

  if not queue:
    print('Queue not found, creating it')
    # Otherwise create the queue
    # Construct the fully qualified location path.
    q_parent = '/'.join(queue_config['name'].split('/')[:-2])
    queue = client.create_queue(request={
        'parent': q_parent,
        'queue': queue_config
    })

  else:
    queue = client.update_queue(request={
        'queue': queue_config
    })

  return queue

def _build_file_message(parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, extra_parameters, target_platform):
  """Creates a JSON payload representing the file to preocess.

  Args:
    parent_cid (string): CID associated with the parent file.
    parent_filename (string): file name of the parent file.
    parent_filepath (string): file path for the parent file.
    parent_numchunks (integer): number of child chunks created for the parent.
    parent_numrows (integer): number of rows in the parent file.
    child_filename (string): file name of the child file.
    child_numrows (integer): number of rows in the child file.
    parent_date (string): date when the file was created
    processing_date (string): date when the file is being processed
    extra_parameters: string array representing the extra parameters for the platform
    target_platform: string representing the platform to send the task to

  Returns:
      JSON payload
  """

  return {
      'date': processing_date,
      'target_platform': target_platform,
      'extra_parameters': extra_parameters,
      'parent': {
          'cid': parent_cid.replace('-', ''),
          'file_name': parent_filename,
          'file_path': parent_filepath,
          'file_date': parent_date,
          'total_files': parent_numchunks,
          'total_rows': parent_numrows,
      },
      'child': {
          'file_name': os.path.basename(child_filename),
          'num_rows': child_numrows
      }
  }

def _create_new_task(client, queue, project, location, queue_name, parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, url, extra_parameters, service_account, target_platform):
  """Creates a new task in Cloud Tasks to process a chunk of conversions.

  Args:
    client: the cloud taks client
    queue (Queue): cloud task queue to insert the task into
    project (string): name of the GCP project.
    location (string): location where Cloud Tasks is running.
    queue_config (Dict[str, Any]): config for the cloud task
    parent_cid (string): CID associated with the parent file.
    parent_filename (string): file name of the parent file.
    parent_filepath (string): file path for the parent file.
    parent_numchunks (integer): number of child chunks created for the parent.
    parent_numrows (integer): number of rows in the parent file.
    child_filename (string): file name of the child file.
    child_numrows (integer): number of rows in the child file.
    parent_date (string): date when the file was created
    processing_date (string): date when the file is being processed
    url (string): url of the cloud function to be triggered by the task.
    extra_parameters: string array representing the extra parameters for the platform
    service_account: string representing the service account to use for auth
    target_platform: string representing the platform to send the task to

  Returns:
      None
  """
  task_id = f'{os.path.basename(child_filename)}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
  task_id = hashlib.md5(task_id.encode('utf-8')).hexdigest()
  task_name = client.task_path(project,
                               location,
                               queue.name.split('/')[-1],
                               task_id)
  # Construct the request body.
  task = {
      'name': task_name,
      'http_request': {  # Specify the type of request.
          'http_method': tasks_v2.HttpMethod.POST,
          'url': url,  # The full url path that the task will be sent to.
          'oidc_token': {
            'service_account_email': service_account,
          },
      }
  }

  payload_json = _build_file_message(parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, extra_parameters, target_platform)

  # payload_json = {
  #    'date': processing_date,
  #    'target_platform': target_platform,
  #    'extra_parameters': extra_parameters,
  #    'parent': {
  #        'cid': parent_cid.replace('-', ''),
  #        'file_name': parent_filename,
  #        'file_path': parent_filepath,
  #        'file_date': parent_date,
  #        'total_files': parent_numchunks,
  #        'total_rows': parent_numrows,
  #    },
  #    'child': {
  #        'file_name': os.path.basename(child_filename),
  #        'num_rows': child_numrows
  #    }
  #}

  print(payload_json)
  # Add the payload
  payload = json.dumps(payload_json)

  # Specify http content-type to application/json
  task['http_request']['headers'] = {'Content-type': 'application/json'}

  # The API expects a payload of type bytes.
  converted_payload = payload.encode()

  # Add the payload to the request.
  task['http_request']['body'] = converted_payload

  # Use the client to build and send the task.
  client.create_task(request={'parent': queue.name, 'task': task})
  # print('Created task {}'.format(response.name))


def _write_chunk_to_blob(storage_client, bucket_name, blob_name, data):
  """Function to write a list of strings into a blob within a GCS bucket.

  Args:
    storage_client: Google Cloud Storage client
    bucket_name (string): name of the source bucket
    blob_name (string): name of the blob to create
    data (list): list of strings to be written to the blob

  Returns:
      None
  """
  # CSV is created in /tmp with a random name and then uploaded to GCS
  random_filename = '/tmp/' + ''.join(
      random.choice(string.ascii_lowercase) for i in range(16)) + '.csv'
  with open(random_filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(data)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(random_filename)
  # print('Wrote chunk to file {}'.format(blob_name))


def _read_csv_from_blob(storage_client, bucket_name, blob_name):
  """Function to read a blob containing a CSV file and return it as an array.

  Args:
    storage_client: Google Cloud Storage client
    bucket_name (string): name of the source bucket
    blob_name (string): name of the file to move

  Returns:
    Decoded blob with the contents of the file
  """
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  downloaded_blob = blob.download_as_string()
  decoded_blob = downloaded_blob.decode('utf-8')
  return decoded_blob


def _mv_blob(storage_client,bucket_name, blob_name, new_bucket_name, new_blob_name):
  """Function for moving files between directories or buckets in GCP.

  Args:
    storage_client: Google Cloud Storage client
    bucket_name (string): name of the source bucket
    blob_name (string): name of the file to move
    new_bucket_name (string): name of target bucket (can be same as original)
    new_blob_name (string): name of file in target bucket

  Returns:
      None
  """
  source_bucket = storage_client.get_bucket(bucket_name)
  source_blob = source_bucket.blob(blob_name)
  destination_bucket = storage_client.get_bucket(new_bucket_name)

  # copy to new destination
  source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
  # delete from source
  source_blob.delete()

  print(f'File moved from {blob_name} to {new_blob_name}')


def _file_slicer_worker(client, storage_client, file_name, bucket_name, max_chunk_lines, project,
                        location, queue_config, invoker_url, service_account, target_platform):
  """Splits a file into smaller chunks with a specific number of lines.

  Args:
      client (tasks_v2.CloudTasksClient): the Cloud Tasks client
      storage_client: Google Cloud Storage Client
      file_name (string): Name of the file to be splitted in chunks.
      bucket_name (string): Name of Cloud Storage Bucket containing the file.
      max_chunk_lines (integer): Max number of lines to write into each chunk.
      project (string): name of the GCP project
      location (string): location of the GCP project
      queue_config (Dict[str, Any]): name of the Cloud Tasks queue to use
      invoker_url (string): full url to the gAds Invoker Cloud Function
      service_account (string): represents the SA for authentication
      target_platform (string): the platform to send the tasks to

  Returns:
      None
  """

  # Init Cloud Task Queue
  queue = _upsert_queue(client, queue_config)
  # Move file to processing folder
  now = datetime.datetime.now()
  processing_date = now.strftime('%Y%m%d')
  parent_filename = os.path.basename(file_name)
  parent_filepath = os.path.dirname(file_name)
  # Extract the CID from the filename. Structure is
  #     <platform>_<free-text-without-underscore>_<cid>_<login-cid>_<conv-definition-cid>_<YYYYMMDD>*.csv

  parent_cid, parent_date = _extract_info_from_filename(parent_filename)

  # Load file in memory, read line by line and create chunks of a specific size
  conversions_blob = _read_csv_from_blob(storage_client, bucket_name, file_name)
  # print('Conversions blob: {}'.format(conversions_blob))
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  # print('Conversions list: {}'.format(conversions_list))
  parent_numrows = sum(1 for row in conversions_list) -1
  # print('Parent_numrows is {}'.format(parent_numrows))
  conversions_list = csv.reader(io.StringIO(conversions_blob))

  num_rows = 0
  num_chunks = 0
  chunk_buffer = []
  extra_params = []
  header = []
  chunk_lines = 0
  parent_numchunks = None

  for conversion_info in conversions_list:
    num_rows = num_rows + 1


    if num_rows > 2:
      chunk_buffer.append(conversion_info)
      chunk_lines += 1
      if not parent_numchunks:
        parent_numchunks = math.ceil(parent_numrows / max_chunk_lines)
    else:
      if num_rows == 1:
        extra_params = _get_file_parameters(conversion_info)
        # No parameters line, first row is the header
        if len(extra_params) == 0:
          header = conversion_info
        else:
          parent_numrows = parent_numrows -1
      else:
        if num_rows == 2:
          # If parameters line, was present, second row is header
          if len(extra_params) > 0:
            header = conversion_info
          else: # else it's a conversion line
            chunk_buffer.append(conversion_info)
            chunk_lines += 1


    if (chunk_lines > 0) and (chunk_lines % max_chunk_lines == 0):
      # print(f'{chunk_lines} % {max_chunk_lines}')
      num_chunks = num_chunks + 1
      # print('New chunk created, total number for now is {}'.format(num_chunks))
      child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
      child_numrows = len(chunk_buffer)
      chunk_buffer.insert(0, header)
      _write_chunk_to_blob(storage_client, bucket_name, child_filename, chunk_buffer)
      _create_new_task(client, queue, project, location, queue_config,
                       parent_cid, parent_filename, parent_filepath,
                       parent_numchunks, parent_numrows, child_filename,
                       child_numrows, parent_date, processing_date, invoker_url,
                       extra_params, service_account, target_platform)
      chunk_lines = 0
      chunk_buffer = []

  if chunk_buffer:
    num_chunks = num_chunks + 1
    child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
    child_numrows = len(chunk_buffer)
    chunk_buffer.insert(0, header)
    _write_chunk_to_blob(storage_client, bucket_name, child_filename, chunk_buffer)
    _create_new_task(client, queue, project, location, queue_config, parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, invoker_url,
                     extra_params, service_account, target_platform)

  print('Wrote %s chunks for %s' %
        (format(num_chunks), format(parent_filename)))

  new_file_name = f'{processing_date}/processed/{parent_filename}'
  _mv_blob(storage_client, bucket_name, file_name, bucket_name, new_file_name)

def _extract_info_from_filename(filename: str) -> (str, str):
  """Extracts the parent cid and date from file name

    File format is as follows:

    <platform>_<free-text-without-underscore>_<cid>_<login-cid>_<conv-definition-cid>

  Args:
    filename: the name of the file

  Returns:
        The cid of the file
        The date of the file
  """
  arr = filename.split('_')

  if len(arr) < 6:
    raise Exception('File name format is not correct')

  return (arr[2].replace('-', ''),
          arr[5].replace('-', '')
          )


def _get_target_platform(file_name: str) -> str:
  """Returns the platform name from the file name.

     The first token of the file name will correspond to the target platform identifier.

  Args:
      file_name: the name of the input file to the slicer

  Returns:
      A string representing the target platform
  """
  return file_name.split('_')[0].lower()

def _get_invoker_url(platform: str,
                     location: str,
                     project: str,
                     deployment_name: str,
                     solution_prefix: str) -> str:

  """Returns the url to invoke the cloud function for the specific platform.

  Args:
      platform: the target platform
      location: the project location of the cloud function
      project: the project where the cloud function is deployed
      deployment_name: the name given to the deployment
      solution_prefix: the prefix to identify the solution deployed

  Returns:
      The URL to invoke the cloud function
  """
  return f'https://{location}-{project}.cloudfunctions.net/{deployment_name}_{solution_prefix}_{platform}_invoker'


def _get_queue_config(client: tasks_v2.CloudTasksClient,
                      project: str,
                      deployment_name: str,
                      solution_prefix: str,
                      location: str,
                      platform: str,
                      config: Dict[str, Any]) -> Dict[str, Any]:

  min_backoff = duration_pb2.Duration()
  min_backoff.seconds = config['queue_config']['retry_config']['min_backoff']

  max_backoff = duration_pb2.Duration()
  max_backoff.seconds = config['queue_config']['retry_config']['max_backoff']

  # With the former values for min and max_backoff, plus 3 max_doublings,
  # retries happen after 10, 20, 40, 80, 160, 240, 300, 300, 300 and 300
  # seconds, respectively

  max_retry = duration_pb2.Duration()
  # Max retry = 10 + 20 + 40 + 8 + 160 + 240 + 300 + 300 + 300 + 300
  max_retry.seconds = config['queue_config']['retry_config']['max_retry_duration']

  queue_config = {
      'name': client.queue_path(project, location, config['queue_config']['name']),
      'rate_limits': {
          'max_dispatches_per_second': config['queue_config']['rate_limits']['max_dispatches_per_second'],
          'max_concurrent_dispatches': config['queue_config']['rate_limits']['max_concurrent_dispatches'],
      },
      'retry_config': {
          'max_attempts': config['queue_config']['retry_config']['max_attempts'],
          'max_retry_duration': max_retry,
          'min_backoff': min_backoff,
          'max_backoff': max_backoff,
          'max_doublings': config['queue_config']['retry_config']['max_doublings'],
      }
  }

  return queue_config

def _read_platform_config_from_secret(project_id: str, secret_id: str) -> Dict[str, Any]:
  """Gets the config for the platform.

  Args:
      project_id: the project name where the secret is defined
      secret_id: the string representing the id to address the secret with the config

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

def file_slicer(data, context=Optional[Context]):
  """Background Cloud Function to be triggered by Cloud Storage.

     This function will split a big CSV file into smaller CSVs each of them
     containing a specific number of lines.

  Args:
      data (dict): The Cloud Functions event payload.
      context (google.cloud.functions.Context): The context of the request

  Returns:
      None
  """
  del context  # Not used
  file_name = data['name']
  bucket = data['bucket']
  if not all(elem in os.environ for elem in [
      'DEFAULT_GCP_PROJECT', 'DEFAULT_GCP_REGION',
      'DEPLOYMENT_NAME', 'SOLUTION_PREFIX', 'SERVICE_ACCOUNT', 'STORE_RESPONSE_STATS_TOPIC'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)
  if file_name.endswith('.csv') and file_name.startswith('input/'):
    # Create a client.
    client = tasks_v2.CloudTasksClient()
    storage_client = storage.Client()
    print('Processing file %s' % file_name)

    project = os.environ['DEFAULT_GCP_PROJECT']
    location = os.environ['DEFAULT_GCP_REGION']
    deployment_name = os.environ['DEPLOYMENT_NAME']
    solution_prefix = os.environ['SOLUTION_PREFIX']
    service_account = os.environ['SERVICE_ACCOUNT']
    reporting_topic = os.environ['STORE_RESPONSE_STATS_TOPIC']
    full_path_topic = f'{deployment_name}.{solution_prefix}.{reporting_topic}'


    try:

      target_platform = _get_target_platform(os.path.basename(file_name))

      config = _read_platform_config_from_secret(project,
                    f'{deployment_name}_{solution_prefix}_{target_platform}_config')

      max_chunk_lines = config['slicer']['max_chunk_lines']
      queue_config = _get_queue_config(client,
                                       project,
                                       deployment_name,
                                       solution_prefix,
                                       location,
                                       target_platform,
                                       config)
      invoker_url = _get_invoker_url(target_platform,
                     location,
                     project,
                     deployment_name,
                     solution_prefix)
      _file_slicer_worker(client, storage_client, file_name, bucket, max_chunk_lines, project, location,
                          queue_config, invoker_url, service_account, target_platform)
    except Exception:
      now = datetime.datetime.now()
      processing_date = now.strftime('%Y%m%d')
      file_name = os.path.basename(file_name)
      source_file_name = f'input/{file_name}'
      target_file_name = f'{processing_date}/failed/{file_name}'
      _mv_blob(storage_client, bucket, source_file_name, bucket, target_file_name)
      parent_cid, parent_date = _extract_info_from_filename(file_name)
      input_json = _build_file_message(parent_cid,
                     file_name, os.path.dirname(file_name), -1,
                     -1, file_name, -1, parent_date,
                     processing_date, None, target_platform)

      pubsub_payload = _add_errors_to_input_data(
        input_json, -1)
      _send_pubsub_message(project, full_path_topic, pubsub_payload)

      raise

def main(argv: Sequence[str]) -> None:
  """Main function for testing using the command line.

  Args:
      argv (typing.Sequence): argument list

  Returns:
      None
  """
  data = {
    "bucket": os.environ['DEFAULT_GCS_BUCKET'],
    "name": argv[1]
    }
  file_slicer(data=data)


if __name__ == '__main__':
  app.run(main)
