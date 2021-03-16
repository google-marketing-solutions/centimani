"""Google Cloud function that loads the Datastore data into BQ"""

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
import pandas as pd
import numpy as np
import pytz

from typing import Any, Dict, Optional
from google.cloud import bigquery
from google.cloud.functions_v1.context import Context
from google.cloud import pubsub_v1 as pubsub
# from google.cloud import firestore_v1 as firestore
from google.cloud import datastore as store

DEFAULT_GCP_PROJECT = os.getenv("DEFAULT_GCP_PROJECT", "")
BQ_REPORTING_DATASET = os.getenv("BQ_REPORTING_DATASET", "")
BQ_REPORTING_TABLE = os.getenv("BQ_REPORTING_TABLE", "")

def _get_data_from_datastore(current_date: str) -> pd.DataFrame:
  """Extracts all entities processed at current_date.

    Args:
        current_date:  string representing the current date in YYYYMMDD format
    Returns:
  """
  
  db = store.Client(DEFAULT_GCP_PROJECT)
  ancestor = db.key("processing_date", current_date)
  query = db.query(kind="child_file", ancestor=ancestor)
  results = list(query.fetch())

  return pd.DataFrame(results) 


def _write_to_bigquery(df: pd.DataFrame, table_name: str):
  """Writes the given dataframe into the BQ table.

    Args:
      df: A pandas dataframe representing the data to be written
      table_name: A string representing the full path of the metadata BQ table
  """

  dataframe = df

  client = bigquery.Client()

  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_TRUNCATE"
  job_config.schema = _get_bq_schema()

  job = client.load_table_from_dataframe(
      dataframe, table_name, job_config=job_config)
  job.result()

  table = client.get_table(table_name)
  print("Loaded {} rows and {} columns to {}".format(table.num_rows,
                                                     len(table.schema),
                                                     table_name))


def _get_bq_schema():
  return [
         bigquery.SchemaField("cid","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("processing_date","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("parent_file_name","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("parent_file_path","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("parent_file_date","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("parent_total_files","INT64",mode="REQUIRED")
        ,bigquery.SchemaField("parent_total_rows","INT64",mode="REQUIRED")
        ,bigquery.SchemaField("child_file_name","STRING",mode="REQUIRED")
        ,bigquery.SchemaField("child_num_rows","INT64",mode="REQUIRED")
        ,bigquery.SchemaField("child_num_errors","INT64",mode="REQUIRED")
        ]


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
  date = datetime.date.today().strftime("%Y%m%d")
  table_name = f"{DEFAULT_GCP_PROJECT}.{BQ_REPORTING_DATASET}.{BQ_REPORTING_TABLE}_{date}"
 
  df = _get_data_from_datastore(date)
 
  if len(df)>0:
    _write_to_bigquery(df, table_name)

def _test_main():
  data = {}
  main(
      event={"data": base64.b64encode(bytes(json.dumps(data).encode("utf-8")))})

def _test_get_data_from_datastore():
  df = _get_data_from_datastore("20210316")


if __name__ == "__main__":
  _test_main()
