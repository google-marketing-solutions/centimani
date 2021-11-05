"""Google Cloud function that processes the stores API responses."""

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

import base64
import datetime
import json
import os
import time
from typing import Any, Dict, Optional

from google.cloud import datastore as store
from google.cloud.functions_v1.context import Context
import pytz

PROJECT_ID = os.getenv("PROJECT_ID", "")
MAX_RETRIES = 3
SLEEP_IN_SECONDS = 5


def _upsert_processing_date_in_datastore(db: store.client.Client,
                                         data: Dict[str, Any]) -> store.key.Key:
  """Creates/updates the date entity.

  Args:
    db: Datastore client.
    data: a dictionary containing the information to store in Datastore.
  Returns:
    A datastore key for the processing date
  """

  date_key = db.key("processing_date", data["processing_date"])

  with db.transaction():
    processing_date = store.Entity(key=date_key)
    db.put(processing_date)

  return date_key


def _upsert_child_file_in_datastore(
    db: store.client.Client, data: Dict[str, Any],
    parent_key: store.key.Key) -> store.key.Key:
  """Create and initialises the chile file datastore structure.

  Args:
    db:  Datastore client.
    data: a dictionary containing the information to store in Datastore.
    parent_key: key at the parent level.
  Returns:
    A datastore key pointing at the child structure
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
  """Create and initialises the datastore structure.

  Args:
    db:  Datastore client.
    data: a dictionary containing the information to store in Datastore.
  Returns:
    None
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
  db = store.Client(PROJECT_ID)
  _insert_data_in_datastore(db, _build_data_for_store(input_data))


def _test_main():
  """Function provided for functionality testing from the command line.

  Args:
    None
  Returns:
    None
  """
  data = {
      "date": "YYYYMMDD",
      "target_platform": "gads",
      "extra_parameters": [
          "Parameters:TimeZone=Europe/Madrid",
          "",
          "",
          "",
          ""
      ],
      "parent": {
          "cid": "1234",
          "file_name": "file.csv",
          "file_path": "input",
          "file_date": "20210420",
          "total_files": 100,
          "total_rows": 25000
      },
      "child": {
          "file_name": "file.csv---1",
          "num_rows": 250,
          "num_errors": 48,
          "errors": [{
              "code": "conversion_upload_error: UNAUTHORIZED_CUSTOMER",
              "message": "Click owned by a non managed account.",
              "count": 48
          }]
      }
  }

  main(
      event={"data": base64.b64encode(bytes(json.dumps(data).encode("utf-8")))})


if __name__ == "__main__":
  _test_main()
