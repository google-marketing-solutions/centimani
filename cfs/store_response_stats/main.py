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
from google.cloud import datastore as store
import pytz

DEFAULT_GCP_PROJECT = os.getenv("DEFAULT_GCP_PROJECT", "")
MAX_RETRIES = 3
SLEEP_IN_SECONDS = 5

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

def _build_data_for_store(data: Dict[str, Any]) -> Dict[str, Any]:

  child_errors = {}

  if "errors" in data["child"]:
    child_errors = data["child"]["errors"]

  return {
    "cid": data["parent"]["cid"],
    "processing_date": data["date"],
    "target_platform": data["target_platform"],
    "parent_file_name": data["parent"]["file_name"],
    "parent_file_path": data["parent"]["file_path"],
    "parent_file_date": data["parent"]["file_date"],
    "parent_total_files": data["parent"]["total_files"],
    "parent_total_rows": data["parent"]["total_rows"],
    "child_file_name": data["child"]["file_name"],
    "child_num_rows": data["child"]["num_rows"],
    "child_num_errors": data["child"]["num_errors"],
    "child_errors": child_errors,
    "last_processed_timestamp": datetime.datetime.now(pytz.utc)
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
  _insert_data_in_datastore(db, 
    _build_data_for_store(input_data)
  )


def _test_main():
  data = {
      "date": "20210316",
      "target_platform": "GAds",
      "parent": {
          "cid": "1234",
          "file_name": "parent_file_1",
          "file_path": "gs://a/b/c",
          "file_date": "20210310",
          "total_files": 10,
          "total_rows": 200
      },
      "child": {
          "file_name": "child_file_2",
          "num_rows": 20,
          "num_errors": 10,
          "errors": {}
      },
      "conversions_api_response": {}
  }
  data = {
      "date": "20210316",
      "target_platform": "GAds",
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
          "num_rows": 20,
          "num_errors": 10,
          "errors": {'ERROR_TYPE_1': 5,
                     'ERROR_TYPE_2': 7}
      },
      "conversions_api_response": {}
  }
  
  main(
      event={"data": base64.b64encode(bytes(json.dumps(data).encode("utf-8")))})


if __name__ == "__main__":
  _test_main()