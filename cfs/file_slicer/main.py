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

import csv
import datetime
import io
import os
import sys
from typing import Sequence

from absl import app
from google.cloud import storage


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
  bucket = client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_string(''.join([str(elem) for elem in data]))


def _read_csv_from_blob(bucket_name, blob_name):
  """Function to read a blob containing a CSV file and return it as an array.

  Args:
    bucket_name (string): name of the source bucket
    blob_name (string): name of the file to move
  Returns:
    List of strings with the contents of the file
  """
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  downloaded_blob = blob.download_as_string()
  decoded_blob = downloaded_blob.decode('utf-8')
  return csv.reader(io.StringIO(decoded_blob))


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


def _file_slicer_worker(file_name, bucket_name, max_chunk_lines):
  """Splits a file into smaller chunks with a specific number of lines.

  Args:
      file_name (string): Name of the file to be splitted in chunks.
      bucket_name (string): Name of Cloud Storage Bucket containing the file.
      max_chunk_lines (integer): Max number of lines to write into each chunk.
  Returns:
      None
  """
  # Move file to processing folder
  now = datetime.datetime.now()
  date_mark = now.strftime('%Y%m%d')
  base_file_name = os.path.basename(file_name)
  new_file_name = f'{date_mark}/processing/{base_file_name}'
  _mv_blob(bucket_name, file_name, bucket_name, new_file_name)

  # Load file in memory, read line by line and create chunks of a specific size
  conversion_list = _read_csv_from_blob(bucket_name, new_file_name)

  num_lines = 0
  num_slices = 0
  chunk_buffer = []
  for conversion_info in conversion_list:
    num_lines = num_lines + 1
    chunk_buffer.append(conversion_info)
    if num_lines % max_chunk_lines == 0:
      num_slices = num_slices + 1
      target_filename = f'{date_mark}/slices_processing/{base_file_name}---{format(num_slices)}'
      _write_chunk_to_blob(bucket_name, target_filename, chunk_buffer)
      chunk_buffer = []
  if chunk_buffer:
    num_slices = num_slices + 1
    target_filename = f'{date_mark}/slices_processing/{base_file_name}---{format(num_slices)}'
    _write_chunk_to_blob(bucket_name, target_filename, chunk_buffer)
  print('Wrote %s chunks for %s' % (format(num_slices), format(base_file_name)))


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
  if 'MAX_CHUNK_LINES' not in os.environ:
    print('Cannot proceed, there are missing input values, '
          'please make sure you set all the environment variables correctly.')
    sys.exit(1)
  if filename.endswith('.csv') and filename.startswith('input/'):
    print('Processing file %s' % filename)
    max_chunk_lines = int(os.environ['MAX_CHUNK_LINES'])
    _file_slicer_worker(filename, bucket, max_chunk_lines)
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
    raise app.UsageError('Please pass filename, bucket and max_chunk_lines.')
  _file_slicer_worker(argv[1], argv[2], argv[3])

if __name__ == '__main__':
  app.run(main)
