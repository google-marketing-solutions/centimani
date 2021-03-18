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
from typing import Any, Dict, Sequence, Optional

from absl import app
from google.cloud import storage
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
from google.cloud.functions_v1.context import Context
from google.protobuf import duration_pb2

def _get_file_parameters(csv_line: str) -> Dict[str, Any]:
  if 'Parameters' in csv_line[0]:
    return csv_line
  else:
    return []
  
def _get_queue_config(client: tasks_v2.CloudTasksClient, project: str,
                      location: str, queue_name: str,
                      max_dispatches_per_second: int,
                      max_concurrent_dispatches: int, max_attempts: int,
                      max_retry_seconds: int, min_backoff_seconds: int,
                      max_backoff_seconds: int,
                      max_doublings: int) -> Dict[str, Any]:

  min_backoff = duration_pb2.Duration()
  min_backoff.seconds = min_backoff_seconds

  max_backoff = duration_pb2.Duration()
  max_backoff.seconds = max_backoff_seconds

  # With the former values for min and max_backoff, plus 3 max_doublings,
  # retries happen after 10, 20, 40, 80, 160, 240, 300, 300, 300 and 300
  # seconds, respectively

  max_retry = duration_pb2.Duration()
  # Max retry = 10 + 20 + 40 + 8 + 160 + 240 + 300 + 300 + 300 + 300
  max_retry.seconds = max_retry_seconds

  queue_config = {
      'name': client.queue_path(project, location, queue_name),
      'rate_limits': {
          'max_dispatches_per_second': max_dispatches_per_second,
          'max_concurrent_dispatches': max_concurrent_dispatches,
      },
      'retry_config': {
          'max_attempts': max_attempts,
          'max_retry_duration': max_retry,
          'min_backoff': min_backoff,
          'max_backoff': max_backoff,
          'max_doublings': max_doublings,
      }
  }

  return queue_config


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
    q_parent = queue_config['name'].split("/")[:-1]
    # Construct the create queue request.

    min_backoff = duration_pb2.Duration()
    min_backoff.seconds = 10

    max_backoff = duration_pb2.Duration()
    max_backoff.seconds = 300

    # With the former values for min and max_backoff, plus 3 max_doublings,
    # retries happen after 10, 20, 40, 80, 160, 240, 300, 300, 300 and 300
    # seconds, respectively

    max_retry = duration_pb2.Duration()
    # Max retry = 10 + 20 + 40 + 8 + 160 + 240 + 300 + 300 + 300 + 300
    max_retry.seconds = 1750
    queue = client.create_queue(request={
        'parent': q_parent,
        'queue': queue_config
    })

  else:
    queue = client.update_queue(request={
        'queue': queue_config
    })

  return queue

def _create_new_task(client, queue, project, location, queue_name, parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, url, extra_parameters):
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

  Returns:
      None
  """

  # Construct the request body.
  task = {
      'http_request': {  # Specify the type of request.
          'http_method': tasks_v2.HttpMethod.POST,
          'url': url,  # The full url path that the task will be sent to.
      }
  }

  payload_json = {
      'date': processing_date,
      'target_platform': 'gads',
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

  # Add the payload
  payload = json.dumps(payload_json)

  # Specify http content-type to application/json
  task['http_request']['headers'] = {'Content-type': 'application/json'}

  # The API expects a payload of type bytes.
  converted_payload = payload.encode()

  # Add the payload to the request.
  task['http_request']['body'] = converted_payload

  # Use the client to build and send the task.
  response = client.create_task(request={'parent': queue.name, 'task': task})
  # print('Created task {}'.format(response.name))


def _write_chunk_to_blob(bucket_name, blob_name, data):
  """Function to write a list of strings into a blob within a GCS bucket.

  Args:
    bucket_name (string): name of the source bucket
    blob_name (string): name of the blob to create
    data (list): list of strings to be written to the blob

  Returns:
      None
  """
  client = storage.Client()
  # CSV is created in /tmp with a random name and then uploaded to GCS
  random_filename = '/tmp/' + ''.join(
      random.choice(string.ascii_lowercase) for i in range(16)) + '.csv'
  with open(random_filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(data)
  bucket = client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(random_filename)
  # print('Wrote chunk to file {}'.format(blob_name))


def _read_csv_from_blob(bucket_name, blob_name):
  """Function to read a blob containing a CSV file and return it as an array.

  Args:
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


def _file_slicer_worker(client, file_name, bucket_name, max_chunk_lines, project,
                        location, queue_config, invoker_url):
  """Splits a file into smaller chunks with a specific number of lines.

  Args:
      client (tasks_v2.CloudTasksClient): the Cloud Tasks client
      file_name (string): Name of the file to be splitted in chunks.
      bucket_name (string): Name of Cloud Storage Bucket containing the file.
      max_chunk_lines (integer): Max number of lines to write into each chunk.
      project (string): name of the GCP project
      location (string): location of the GCP project
      queue_config (Dict[str, Any]): name of the Cloud Tasks queue to use
      invoker_url (string): full url to the gAds Invoker Cloud Function

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

  # Extract the CID from the filename. Structure is XXX_NNN-NNNN-NNN_*.csv
  underscore_pos = parent_filename.find('_')
  rest_of_string = -1 * (len(parent_filename) - underscore_pos - 1)
  cid_tmp = parent_filename[rest_of_string:]
  underscore_pos = cid_tmp.find('_')
  parent_cid = cid_tmp[0:underscore_pos]
  rest_of_string = -1 * (len(cid_tmp) - underscore_pos - 1)
  date_tmp = cid_tmp[rest_of_string:]
  underscore_pos = date_tmp.find('_')
  parent_date = date_tmp[0:underscore_pos]

  new_file_name = f'{processing_date}/processing/{parent_filename}'
  _mv_blob(bucket_name, file_name, bucket_name, new_file_name)

  # Load file in memory, read line by line and create chunks of a specific size
  conversions_blob = _read_csv_from_blob(bucket_name, new_file_name)
  # print('Conversions blob: {}'.format(conversions_blob))
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  # print('Conversions list: {}'.format(conversions_list))
  parent_numrows = sum(1 for row in conversions_blob)
  # print('Parent_numrows is {}'.format(parent_numrows))
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  parent_numchunks = math.ceil(parent_numrows / max_chunk_lines)

  num_rows = 0
  num_chunks = 0
  chunk_buffer = []
  extra_params = []
  header = []
  print('Reading conversions')
  for conversion_info in conversions_list:
    num_rows = num_rows + 1
   
    if num_rows == 1:
      extra_params = _get_file_parameters(conversion_info)
      if len(extra_params) == 0:
        header = conversion_info   
        chunk_buffer.append(header)
    
    if num_rows == 2 and len(extra_params) > 0:
      header = conversion_info   
      chunk_buffer.append(header)

    if num_rows > 2:
      chunk_buffer.append(conversion_info)

    if num_rows % max_chunk_lines == 0:
      num_chunks = num_chunks + 1
      # print('New chunk created, total number for now is {}'.format(num_chunks))
      child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
      child_numrows = len(chunk_buffer)
      _write_chunk_to_blob(bucket_name, child_filename, chunk_buffer)
      _create_new_task(client, queue, project, location, queue_config,
                       parent_cid, parent_filename, parent_filepath,
                       parent_numchunks, parent_numrows, child_filename,
                       child_numrows, parent_date, processing_date, invoker_url,
                       extra_params)
      chunk_buffer = []
      chunk_buffer.append(header)

  if chunk_buffer:
    num_chunks = num_chunks + 1
    child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
    child_numrows = len(chunk_buffer)
    _write_chunk_to_blob(bucket_name, child_filename, chunk_buffer)
    _create_new_task(client, queue, project, location, queue_config, parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows, parent_date,
                     processing_date, invoker_url,
                     extra_params)

  print('Wrote %s chunks for %s' %
        (format(num_chunks), format(parent_filename)))


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
  filename = data['name']
  bucket = data['bucket']
  if not all(elem in os.environ for elem in [
      'MAX_CHUNK_LINES', 'DEFAULT_GCP_PROJECT', 'DEFAULT_GCP_REGION',
      'CT_QUEUE_NAME', 'DEPLOYMENT_NAME', 'SOLUTION_PREFIX', 'CF_NAME_GADS_INVOKER'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)
  if filename.endswith('.csv') and filename.startswith('input/'):
    # Create a client.
    client = tasks_v2.CloudTasksClient()
    print('Processing file %s' % filename)
    max_chunk_lines = int(os.environ['MAX_CHUNK_LINES'])
    project = os.environ['DEFAULT_GCP_PROJECT']
    location = os.environ['DEFAULT_GCP_REGION']
    queue_name = os.environ['CT_QUEUE_NAME']
    deployment_name = os.environ['DEPLOYMENT_NAME']
    solution_prefix = os.environ['SOLUTION_PREFIX']
    cf_name_invoker = os.environ['CF_NAME_GADS_INVOKER']
    invoker_url = f'https://{location}-{project}.cloudfunctions.net/{deployment_name}_{solution_prefix}_{cf_name_invoker}'
    max_dispatches_per_second = int(
        os.environ['CT_QUEUE_MAX_DISPATCHES_PER_SECOND'])
    max_concurrent_dispatches = int(
        os.environ['CT_QUEUE_MAX_CONCURRENT_DISPATCHES'])
    max_attempts = int(os.environ['CT_QUEUE_MAX_ATTEMPTS'])
    max_retry_seconds = int(os.environ['CT_QUEUE_MAX_RETRY_SECONDS'])
    min_backoff_seconds = int(os.environ['CT_QUEUE_MIN_BACKOFF_SECONDS'])
    max_backoff_seconds = int(os.environ['CT_QUEUE_MAX_BACKOFF_SECONDS'])
    max_doublings = int(os.environ['CT_QUEUE_MAX_DOUBLINGS'])

    try:
      queue_config = _get_queue_config(client, project, location, queue_name,
                                      max_dispatches_per_second,
                                      max_concurrent_dispatches, max_attempts,
                                      max_retry_seconds, min_backoff_seconds,
                                      max_backoff_seconds, max_doublings)
      _file_slicer_worker(client, filename, bucket, max_chunk_lines, project, location,
                          queue_config, invoker_url)
    except Exception as e:
      now = datetime.datetime.now()
      processing_date = now.strftime('%Y%m%d')
      filename = os.path.basename(filename)
      file_name = f'{processing_date}/processing/{filename}'
      new_file_name = f'{processing_date}/failed/{filename}'
      _mv_blob(bucket, file_name, bucket, new_file_name)

  else:
    print('Bypassing file %s' % filename)


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
  #if len(argv) != 3:
  #  raise app.UsageError('Please make sure you are passing all required params')
  #else:
    # Params expected are filename, bucket, max_chunk_lines, project, location,
    # queue_name and invoker CF full url
  #  _file_slicer_worker(
  #      argv[1], argv[2], argv[3], argv[4], argv[5],
  #      _get_queue_config(
  #          project=argv[4], location=argv[5], queue_name=argv[6]), argv[7])


if __name__ == '__main__':
  app.run(main)
