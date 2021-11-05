# Common Configuration

# Configuration file for the generic part of the framework
#
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

PROJECT_ID  = ""
# Use `gcloud compute regions list` to find out the different values
REGION = "europe-west1"

DEPLOYMENT_NAME = "mydeployment"
SOLUTION_PREFIX = "centimani"
SERVICE_ACCOUNT = "centimani-sa"

BQ_REPORTING_DATASET = "centimani_reporting_dataset"
BQ_REPORTING_TABLE   = "daily_results"
BQ_GCP_BROAD_REGION  = "EU"

# Remember that Cloud Storage bucket names need to be unique across all GCP Projects.
INPUT_GCS_BUCKET  = ""
OUTPUT_GCS_BUCKET = ""
BUILD_GCS_BUCKET  = ""

# These names will be automatically prefixed by $DEPLOYMENT_NAME.$SOLUTION_PREFIX.
REPORTING_DATA_EXTRACTOR_TOPIC = "reporting_data_extractor_topic"
STORE_RESPONSE_STATS_TOPIC = "store_response_stats_topic"

CREATE_FIRESTORE = true
# Firestore regions available are `us-central` and `europe-west`
FIRESTORE_REGION = "europe-west"

CACHE_TTL_IN_HOURS = "2"