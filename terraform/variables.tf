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

variable "PROJECT_ID" {
  type        = string
  description = "GCP Project ID."
}
variable "REGION" {
  type        = string
  description = "GCP region https://cloud.google.com/compute/docs/regions-zones."
  default     = "europe-west1"
}

variable "DEPLOYMENT_NAME" {
  type        = string
  description = "Solution name to add to the Cloud Functions, secrets and scheduler names."
  default     = "centimani-sa"
}
variable "SOLUTION_PREFIX" {
  type        = string
  description = "Prefix to add to the Cloud Functions, secrets and scheduler names."
  default     = "centimani"
}

variable "SERVICE_ACCOUNT" {
  type        = string
  description = "Service Account for running Centimani."
  default     = "centimani-sa"
}

variable "BQ_REPORTING_DATASET" {
  type        = string
  description = "BigQuery dataset to store reporting results."
  default     = "centimani_reporting"
}
variable "BQ_REPORTING_TABLE" {
  type        = string
  description = "BigQuery table to store reporting results."
  default     = "daily_results"
}
variable "BQ_GCP_BROAD_REGION" {
  type        = string
  description = "Region to store BigQuery data."
  default     = "eu"
}

variable "CACHE_TTL_IN_HOURS" {
  type        = string
  description = "TTL (in hours) for operations involving cache."
  default     = "2"
}

variable "INPUT_GCS_BUCKET" {
  type        = string
  description = "Cloud Storage bucket for input data."
}
variable "OUTPUT_GCS_BUCKET" {
  type        = string
  description = "Cloud Storage bucket for output or processed data."
}
variable "BUILD_GCS_BUCKET" {
  type        = string
  description = "Cloud Storage bucket for building cloud functions."
}

variable "REPORTING_DATA_EXTRACTOR_TOPIC" {
  type        = string
  description = "Reporting data extractor topic name."
  default     = "reporting_data_extractor_topic"
}
variable "STORE_RESPONSE_STATS_TOPIC" {
  type        = string
  description = "Store response stats topic name."
  default     = "store_response_stats_topic"
}

variable "REPORTING_DATA_POLLING_CONFIG" {
  type        = string
  description = "Reporting data polling config."
  default     = "*/10 * * * *"
}
variable "SCHEDULER_TIMEZONE" {
  type        = string
  description = "Reporting data polling config."
  default     = "Etc/UTC"
}

variable "FIRESTORE_REGION" {
  type        = string
  description = "Specific region for Firestore database."
  default     = "europe-west"
}

variable "CREATE_FIRESTORE" {
  type        = bool
  description = "Specify if creation of firestore database is needed."
  default     = true
}
