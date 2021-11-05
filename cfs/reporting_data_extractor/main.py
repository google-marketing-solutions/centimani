"""Google Cloud function that loads the Datastore data into BQ."""

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
from typing import Any, Dict, Optional

from google.cloud import bigquery
from google.cloud import datastore as store
from google.cloud.functions_v1.context import Context
import numpy as np
import pandas as pd

PROJECT_ID = os.getenv("PROJECT_ID", "")
BQ_REPORTING_DATASET = os.getenv("BQ_REPORTING_DATASET", "")
BQ_REPORTING_TABLE = os.getenv("BQ_REPORTING_TABLE", "")


def _get_data_from_datastore(current_date: str) -> pd.DataFrame:
  """Extracts all entities processed at current_date.

  Args:
    current_date:  string representing the current date in YYYYMMDD format
  Returns:
    A dataframe containing all data extracted from Datastore
  """

  db = store.Client(PROJECT_ID)
  ancestor = db.key("processing_date", current_date)
  query = db.query(kind="child_file", ancestor=ancestor)
  results = list(query.fetch())

  df = pd.DataFrame(results)
  if "last_processed_timestamp" in df:
    df["last_processed_timestamp"] = df["last_processed_timestamp"].astype(
        np.int64) // 10**9

  return df


def _write_to_bigquery(df: pd.DataFrame, table_name: str):
  """Writes the given dataframe into the BQ table.

  Args:
    df: A pandas dataframe representing the data to be written
    table_name: A string representing the full path of the metadata BQ table
  """

  client = bigquery.Client()
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_TRUNCATE"
  job_config.schema = _get_bq_schema()
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job = client.load_table_from_json(
      json.loads(df.to_json(orient="records")),
      table_name,
      job_config=job_config)
  job.result()


def _get_bq_schema():
  """Returns the schema of the BigQuery table used by the solution.

  Args:
    None
  Returns:
    A list of BigQuery fields.
  """
  return [
      bigquery.SchemaField(name="cid", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="processing_date", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="target_platform", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="parent_file_name", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="parent_file_path", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="parent_file_date", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="parent_total_files", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(
          name="parent_total_rows", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(
          name="child_file_name", field_type="STRING", mode="REQUIRED"),
      bigquery.SchemaField(
          name="child_num_rows", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(
          name="child_num_errors", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(
          name="child_errors",
          field_type="RECORD",
          mode="REPEATED",
          fields=[
              bigquery.SchemaField(
                  name="code", field_type="STRING", mode="NULLABLE"),
              bigquery.SchemaField(
                  name="message", field_type="STRING", mode="NULLABLE"),
              bigquery.SchemaField(
                  name="count", field_type="INTEGER", mode="NULLABLE")
          ]),
      bigquery.SchemaField(
          name="last_processed_timestamp",
          field_type="TIMESTAMP",
          mode="REQUIRED")
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
  del event

  date = datetime.date.today().strftime("%Y%m%d")
  table_name = f"{PROJECT_ID}.{BQ_REPORTING_DATASET}.{BQ_REPORTING_TABLE}_{date}"

  df = _get_data_from_datastore(date)

  if not df.empty:
    _write_to_bigquery(df, table_name)


def _test_main():
  """Function for optional testing of the main function from the command line.

  Args:
    None.
  Returns:
    None
  """
  data = {}
  main(
      event={"data": base64.b64encode(bytes(json.dumps(data).encode("utf-8")))})


def _test_get_data_from_datastore():
  """Used for testing data extraction from Datastore using the command line.

  Args:
    None.
  Returns:
    None
  """
  date = "YYYYMMDD"
  table_name = f"{PROJECT_ID}.{BQ_REPORTING_DATASET}.{BQ_REPORTING_TABLE}_{date}"
  df = _get_data_from_datastore(date)

  if not df.empty:
    _write_to_bigquery(df, table_name)


if __name__ == "__main__":
  _test_get_data_from_datastore()
