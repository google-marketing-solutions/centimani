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
from typing import Sequence

from absl import app
from google.cloud import storage
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
from google.protobuf import duration_pb2

def _create_new_task(project, location, queue_name, parent_cid, parent_filename,
                     parent_filepath, parent_numchunks, parent_numrows,
                     child_filename, child_numrows, parent_date,
                     processing_date, url):
  """Creates a new task in Cloud Tasks to process a chunk of conversions.

  Args:
    project (string): name of the GCP project.
    location (string): location where Cloud Tasks is running.
    queue_name (string): name of the Cloud Tasks queue to use
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
  Returns:
      None
  """
  # Create a client.
  client = tasks_v2.CloudTasksClient()

  # Construct the fully qualified queue name.
  queue_path = client.queue_path(project, location, queue_name)

  # Check if the queue exists
  try:
    queue = client.get_queue(name=queue_path)
  except NotFound:
    queue = None

  if not queue:
    print('Queue not found, creating it')
    # Otherwise create the queue
    # Construct the fully qualified location path.
    q_parent = f'projects/{project}/locations/{location}'
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

    queue = {
        'name': client.queue_path(project, location, queue_name),
        'rate_limits': {
            'max_dispatches_per_second': 4,
            'max_concurrent_dispatches': 4,
        },
        'retry_config': {
            'max_attempts': 10,
            'max_retry_duration': max_retry,
            'min_backoff': min_backoff,
            'max_backoff': max_backoff,
            'max_doublings': 3,

        }
    }
    response = client.create_queue(request={'parent': q_parent, 'queue': queue})

  # Construct the request body.
  task = {
      'http_request': {  # Specify the type of request.
          'http_method': tasks_v2.HttpMethod.POST,
          'url': url,  # The full url path that the task will be sent to.
      }
  }

  payload_json = {
      'date': processing_date,
      'target_platform': 'GAds',
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
  response = client.create_task(request={'parent': queue_path, 'task': task})
  print('Created task {}'.format(response.name))


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
  random_filename = '/tmp/' + ''.join(random.choice(string.ascii_lowercase) for i in range(16)) + '.csv'
  with open(random_filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(data)
  bucket = client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(random_filename)
  print('Wrote chunk to file {}'.format(blob_name))


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


def _file_slicer_worker(file_name, bucket_name, max_chunk_lines, project,
                        location, queue_name, invoker_url):
  """Splits a file into smaller chunks with a specific number of lines.

  Args:
      file_name (string): Name of the file to be splitted in chunks.
      bucket_name (string): Name of Cloud Storage Bucket containing the file.
      max_chunk_lines (integer): Max number of lines to write into each chunk.
      project (string): name of the GCP project
      location (string): location of the GCP project
      queue_name (string): name of the Cloud Tasks queue to use
      invoker_url (string): full url to the gAds Invoker Cloud Function
  Returns:
      None
  """
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
  print('Conversions blob: {}'.format(conversions_blob))
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  print('Conversions list: {}'.format(conversions_list))
  parent_numrows = sum(1 for row in conversions_blob)
  print('Parent_numrows is {}'.format(parent_numrows))
  conversions_list = csv.reader(io.StringIO(conversions_blob))
  parent_numchunks = math.ceil(parent_numrows/max_chunk_lines)

  num_rows = 0
  num_chunks = 0
  chunk_buffer = []
  print('Reading conversions')
  for conversion_info in conversions_list:
    num_rows = num_rows + 1
    chunk_buffer.append(conversion_info)
    if num_rows % max_chunk_lines == 0:
      num_chunks = num_chunks + 1
      print('New chunk created, total number for now is {}'.format(num_chunks))
      child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
      child_numrows = len(chunk_buffer)
      _write_chunk_to_blob(bucket_name, child_filename, chunk_buffer)
      _create_new_task(project, location, queue_name, parent_cid,
                       parent_filename, parent_filepath, parent_numchunks,
                       parent_numrows, child_filename, child_numrows,
                       parent_date, processing_date, invoker_url)
      chunk_buffer = []
  if chunk_buffer:
    num_chunks = num_chunks + 1
    child_filename = f'{processing_date}/slices_processing/{parent_filename}---{format(num_chunks)}'
    child_numrows = len(chunk_buffer)
    _write_chunk_to_blob(bucket_name, child_filename, chunk_buffer)
    _create_new_task(project, location, queue_name, parent_cid,
                     parent_filename, parent_filepath, parent_numchunks,
                     parent_numrows, child_filename, child_numrows,
                     parent_date, processing_date, invoker_url)

  print('Wrote %s chunks for %s' %
        (format(num_chunks), format(parent_filename)))


def file_slicer(data, context):
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
      'CT_QUEUE_NAME', 'DEPLOYMENT_NAME', 'SOLUTION_PREFIX', 'CF_NAME_INVOKER'
  ]):
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)
  if filename.endswith('.csv') and filename.startswith('input/'):
    print('Processing file %s' % filename)
    max_chunk_lines = int(os.environ['MAX_CHUNK_LINES'])
    project = os.environ['DEFAULT_GCP_PROJECT']
    location = os.environ['DEFAULT_GCP_REGION']
    queue_name = os.environ['CT_QUEUE_NAME']
    deployment_name = os.environ['DEPLOYMENT_NAME']
    solution_prefix = os.environ['SOLUTION_PREFIX']
    cf_name_invoker = os.environ['CF_NAME_INVOKER']
    invoker_url = f'https://{location}-{project}.cloudfunctions.net/{deployment_name}_{solution_prefix}_{cf_name_invoker}'
    _file_slicer_worker(filename, bucket, max_chunk_lines, project, location,
                        queue_name, invoker_url)
  else:
    print('Bypassing file %s' % filename)


def main(argv: Sequence[str]) -> None:
  """Main function for testing using the command line.

  Args:
      argv (typing.Sequence): argument list
  Returns:
      None
  """
  if len(argv) != 3:
    raise app.UsageError('Please make sure you are passing all required params')
  else:
    # Params expected are filename, bucket, max_chunk_lines, project, location,
    # queue_name and invoker CF full url
    _file_slicer_worker(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6],
                        argv[7])


if __name__ == '__main__':
  app.run(main)
