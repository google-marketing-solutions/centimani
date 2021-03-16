"""Google Cloud function that process the GADS conv. upload response"""

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

import time
import base64
import datetime
import json
import os
import re
import sys
import uuid
# import pandas as pd
# import numpy as np

from typing import Any, Dict, Optional
from google.cloud.functions_v1.context import Context
from google.cloud import pubsub_v1 as pubsub
# from google.cloud import firestore_v1 as firestore
from google.cloud import datastore as store

import pytz

DEFAULT_GCP_PROJECT = os.getenv("DEFAULT_GCP_PROJECT", "")
MAX_RETRIES = 3
SLEEP_IN_SECONDS = 5


def _is_partial_failure_error_present(response):
  """Checks whether a response message has a partial failure error.

    In Python the partial_failure_error attr is always present on a response
    message and is represented by a google.rpc.Status message. So we can't
    simply check whether the field is present, we must check that the code is
    non-zero. Error codes are represented by the google.rpc.Code proto Enum:
    https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto

    Args:
        response:  A MutateAdGroupsResponse message instance.
    Returns: A boolean, whether or not the response message has a partial
      failure error.
  """
  partial_failure = getattr(response, "partial_failure_error", None)
  code = getattr(partial_failure, "code", None)

  return code != 0


def _process_data(data: Dict[str, Any]) -> Dict[str, Any]:
  """Create and initialises the file firestore structure

  Args:
    data: a dictionary containing the information to store in Firestore

  Returns:
    A dictionary containing the input data + the error data
  """
  
  response = data["conversions_api_response"]
  if _is_partial_failure_error_present(response):
    print("Partial failures occurred. Details will be shown below.\n")

    partial_failure = getattr(response, "partial_failure_error", None)
    error_details = getattr(partial_failure, "details", [])

    for error_detail in error_details:
      failure_message = client.get_type("GoogleAdsFailure", version="v6")
      failure_object = failure_message.FromString(error_detail.value)
    data["child"]["num_errors"] = len(error_details)
  else:
    data["child"]["num_errors"] = 0

  return data


def _upsert_processing_date_in_datastore(db: store.client.Client,
                                         data: Dict[str, Any]) -> store.key.Key:
  """Creates/updates the date entity

  Args:
    db:  Datastore client
    data: a dictionary containing the information to store in Datastore
  """

  date_key = db.key("processing_date", data["processing_date"])

  with db.transaction():
    processing_date = store.Entity(key=date_key)
    db.put(processing_date)

  return date_key

def _upsert_child_file_in_datastore(
    db: store.client.Client, data: Dict[str, Any],
    parent_key: store.key.Key) -> store.key.Key:
  """Create and initialises the chile file datastore structure

  Args:
    db:  Datastore client
    data: a dictionary containing the information to store in Datastore
    date_key: the key of the date
  """
  with db.transaction():
    child_key = db.key("child_file",
                        data["child_file_name"],
                        parent=parent_key)
    child_file = store.Entity(key=child_key)
    child_file.update(data)
    db.put(child_file)

  return child_key


def _insert_data_in_datastore(db: store.Client, data: Dict[str, Any]):
  """Create and initialises the datastore structure

  Args:
    db:  Datastore client
    data: a dictionary containing the information to store in Datastore
  """

  for i in range(MAX_RETRIES):
    try:
      date_key = _upsert_processing_date_in_datastore(db, data)
      _upsert_child_file_in_datastore(db, data, date_key)
      break
    except Exception as e:
      print(e)
      time.sleep(SLEEP_IN_SECONDS)

def _build_data_for_firestore(data: Dict[str, Any]) -> Dict[str, Any]:

    return {
    "cid": data["parent"]["cid"],
    "processing_date": data["date"],
    "parent_file_name": data["parent"]["file_name"],
    "parent_file_path": data["parent"]["file_path"],
    "parent_file_date": data["parent"]["file_date"],
    "parent_total_files": data["parent"]["total_files"],
    "parent_total_rows": data["parent"]["total_rows"],
    "child_file_name": data["child"]["file_name"],
    "child_num_rows": data["child"]["num_rows"],
    "child_num_errors": data["child"]["num_errors"]
  }


def main(event: Dict[str, Any], context=Optional[Context]):
  """Triggers the message processing.

  Args:
    event (dict):  The dictionary with data specific to this type of event. The
      `data` field contains the PubsubMessage message. The `attributes` field
      will contain custom attributes if there are any.
    context (google.cloud.functions.Context): The Cloud Functions event
      metadata. The `event_id` field contains the Pub/Sub message ID. The
      `timestamp` field contains the publish time.
  """
  del context

  data = base64.b64decode(event["data"])
  input_data = json.loads(data)

  db = store.Client(DEFAULT_GCP_PROJECT)

  input_data = _process_data(input_data)
  _insert_data_in_datastore(db, 
    _build_data_for_firestore(input_data)
  )
  


def _test_main():
  data = {
      "date": "20210316",
      "parent": {
          "cid": "1234",
          "file_name": "parent_file_1",
          "file_path": "gs://a/b/c",
          "file_date": "20210310",
          "total_files": 10,
          "total_rows": 200
      },
      "child": {
          "file_name": "child_file_1",
          "num_rows": 20
      },
      "conversions_api_response": {}
  }
  main(
      event={"data": base64.b64encode(bytes(json.dumps(data).encode("utf-8")))})


if __name__ == "__main__":
  _test_main()
