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

data "archive_file" "file_slicer" {
  type        = "zip"
  output_path = ".temp/file_slicer_code_source.zip"
  source_dir  = "${path.module}/../cfs/file_slicer/"
}

resource "google_storage_bucket_object" "file_slicer" {
  name       = "file_slicer_${data.archive_file.file_slicer.output_md5}.zip" # will delete old items
  bucket     = google_storage_bucket.build_bucket.name
  source     = data.archive_file.file_slicer.output_path
  depends_on = [data.archive_file.file_slicer]
}

resource "google_cloudfunctions_function" "file_slicer_function" {
  project     = var.PROJECT_ID
  region      = var.REGION
  depends_on  = [google_project_service.enable_cloudbuild_api]
  name        = "${var.DEPLOYMENT_NAME}_${var.SOLUTION_PREFIX}_file_slicer"
  description = "Centimani File Slicer"
  runtime     = "python37"

  service_account_email = google_service_account.sa.email
  available_memory_mb   = 1024
  source_archive_bucket = google_storage_bucket.build_bucket.name
  source_archive_object = google_storage_bucket_object.file_slicer.name
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.input_bucket.name
  }
  timeout     = 540
  entry_point = "file_slicer"
  environment_variables = {
    PROJECT_ID      = var.PROJECT_ID
    DEPLOYMENT_NAME = var.DEPLOYMENT_NAME
    SOLUTION_PREFIX = var.SOLUTION_PREFIX
    SERVICE_ACCOUNT = google_service_account.sa.email
    REGION          = var.REGION

    BQ_REPORTING_DATASET = var.BQ_REPORTING_DATASET
    BQ_REPORTING_TABLE   = var.BQ_REPORTING_TABLE

    INPUT_GCS_BUCKET  = var.INPUT_GCS_BUCKET
    OUTPUT_GCS_BUCKET = var.OUTPUT_GCS_BUCKET

    STORE_RESPONSE_STATS_TOPIC = var.STORE_RESPONSE_STATS_TOPIC

    CACHE_TTL_IN_HOURS = var.CACHE_TTL_IN_HOURS
  }
}
