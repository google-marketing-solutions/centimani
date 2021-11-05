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

provider "google" {}

provider "archive" {}

# Required API services
resource "google_project_service" "enable_compute_api" {
  project                    = var.PROJECT_ID
  service                    = "compute.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_appengine_api" {
  project                    = var.PROJECT_ID
  service                    = "appengine.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_datastore_api" {
  project                    = var.PROJECT_ID
  service                    = "datastore.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_cloudbuild_api" {
  project                    = var.PROJECT_ID
  service                    = "cloudbuild.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_cloudfunctions_api" {
  project                    = var.PROJECT_ID
  service                    = "cloudfunctions.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_cloudscheduler_api" {
  project                    = var.PROJECT_ID
  service                    = "cloudscheduler.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_pubsub_api" {
  project                    = var.PROJECT_ID
  service                    = "pubsub.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_firestore_api" {
  project                    = var.PROJECT_ID
  service                    = "firestore.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_servicemanagement_api" {
  project                    = var.PROJECT_ID
  service                    = "servicemanagement.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_servicecontrol_api" {
  project                    = var.PROJECT_ID
  service                    = "servicecontrol.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_bigquery_api" {
  project                    = var.PROJECT_ID
  service                    = "bigquery.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_cloudtasks_api" {
  project                    = var.PROJECT_ID
  service                    = "cloudtasks.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}
resource "google_project_service" "enable_secretmanager_api" {
  project                    = var.PROJECT_ID
  service                    = "secretmanager.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}

# Service Account definition
resource "google_service_account" "sa" {
  project      = var.PROJECT_ID
  account_id   = var.SERVICE_ACCOUNT
  display_name = "Service Account for running Centimani"
}
resource "google_project_iam_member" "editor-sa" {
  project = var.PROJECT_ID
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}
resource "google_project_iam_member" "bigquery-admin-sa" {
  project = var.PROJECT_ID
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}
resource "google_project_iam_member" "secretmanager-secretAccessor-sa" {
  project = var.PROJECT_ID
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

# Firestore selection
resource "google_app_engine_application" "app" {
  project       = var.PROJECT_ID
  count         = var.CREATE_FIRESTORE ? 1 : 0
  depends_on    = [google_project_service.enable_appengine_api, google_project_service.enable_firestore_api]
  location_id   = var.FIRESTORE_REGION
  database_type = "CLOUD_FIRESTORE"
}

# BigQuery
resource "google_bigquery_dataset" "dataset" {
  project                     = var.PROJECT_ID
  dataset_id                  = var.BQ_REPORTING_DATASET
  friendly_name               = "Centimani Reporting Dataset"
  location                    = var.BQ_GCP_BROAD_REGION
  depends_on                  = [google_project_service.enable_bigquery_api]
  default_table_expiration_ms = 2592000000 # 30 days
}

# Cloud Storage
resource "google_storage_bucket" "input_bucket" {
  project                     = var.PROJECT_ID
  name                        = var.INPUT_GCS_BUCKET
  location                    = var.REGION
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}
resource "google_storage_bucket" "output_bucket" {
  project                     = var.PROJECT_ID
  name                        = var.OUTPUT_GCS_BUCKET
  location                    = var.REGION
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}
resource "google_storage_bucket" "build_bucket" {
  project                     = var.PROJECT_ID
  name                        = var.BUILD_GCS_BUCKET
  location                    = var.REGION
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}
