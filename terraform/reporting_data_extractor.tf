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

resource "google_pubsub_topic" "reporting_data_extractor" {
  project = var.PROJECT_ID
  name    = "${var.DEPLOYMENT_NAME}.${var.SOLUTION_PREFIX}.${var.REPORTING_DATA_EXTRACTOR_TOPIC}"
}

data "archive_file" "reporting_data_extractor" {
  type        = "zip"
  output_path = ".temp/reporting_data_extractor_code_source.zip"
  source_dir  = "${path.module}/../cfs/reporting_data_extractor/"
}

resource "google_storage_bucket_object" "reporting_data_extractor" {
  name       = "reporting_data_extractor_${data.archive_file.reporting_data_extractor.output_md5}.zip" # will delete old items
  bucket     = google_storage_bucket.build_bucket.name
  source     = data.archive_file.reporting_data_extractor.output_path
  depends_on = [data.archive_file.reporting_data_extractor]
}

resource "google_cloudfunctions_function" "reporting_data_extractor_function" {
  project     = var.PROJECT_ID
  region      = var.REGION
  depends_on  = [google_project_service.enable_cloudbuild_api]
  name        = "${var.DEPLOYMENT_NAME}_${var.SOLUTION_PREFIX}_reporting_data_extractor"
  description = "Centimani Reporting Data Extractor"
  runtime     = "python37"

  service_account_email = google_service_account.sa.email
  available_memory_mb   = 2048
  source_archive_bucket = google_storage_bucket.build_bucket.name
  source_archive_object = google_storage_bucket_object.reporting_data_extractor.name
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.reporting_data_extractor.name
  }
  timeout     = 540
  entry_point = "main"
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


resource "google_cloud_scheduler_job" "job" {
  project     = var.PROJECT_ID
  region      = var.REGION
  name        = "${var.DEPLOYMENT_NAME}_${var.SOLUTION_PREFIX}_reporting_data_extractor"
  description = "Centimani Reporting Data Extractor Schedule"
  schedule    = "*/2 * * * *"
  depends_on  = [resource.google_app_engine_application.app]

  pubsub_target {
    topic_name = google_pubsub_topic.reporting_data_extractor.id
    data       = base64encode("It's Reporting Time!")
  }
}
