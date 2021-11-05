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

locals {
  gads_config = jsondecode(file("${path.module}/../config/gads_config.json"))
}

# Configuration Information
resource "google_secret_manager_secret" "gads-config-secret" {
  project    = var.PROJECT_ID
  provider   = google
  depends_on = [google_project_service.enable_secretmanager_api]
  secret_id  = "${var.DEPLOYMENT_NAME}_${var.SOLUTION_PREFIX}_gads_config"

  replication {
    automatic = true
  }
}
resource "google_secret_manager_secret_version" "gads-config-secret-data" {
  provider    = google
  secret      = google_secret_manager_secret.gads-config-secret.id
  secret_data = jsonencode(local.gads_config)
}

data "archive_file" "gads_invoker" {
  type        = "zip"
  output_path = ".temp/gads_invoker_code_source.zip"
  source_dir  = "${path.module}/../cfs/gads_invoker/"
}

resource "google_storage_bucket_object" "gads_invoker" {
  name       = "gads_invoker_${data.archive_file.gads_invoker.output_md5}.zip" # will delete old items
  bucket     = google_storage_bucket.build_bucket.name
  source     = data.archive_file.gads_invoker.output_path
  depends_on = [data.archive_file.gads_invoker]
}

resource "google_cloudfunctions_function" "gads_invoker_function" {
  project     = var.PROJECT_ID
  region      = var.REGION
  depends_on  = [google_project_service.enable_cloudbuild_api]
  name        = "${var.DEPLOYMENT_NAME}_${var.SOLUTION_PREFIX}_gads_invoker"
  description = "Centimani Google Ads Offline Conversions Uploader Operator"
  runtime     = "python37"

  service_account_email = google_service_account.sa.email
  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.build_bucket.name
  source_archive_object = google_storage_bucket_object.gads_invoker.name
  trigger_http          = true
  timeout               = 540
  entry_point           = "gads_invoker"
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
