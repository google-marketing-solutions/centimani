#!/bin/bash
# Performs cloud function deployment
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


CONFIG_PATH="../../deploy/config.yaml"
HELPERS_PATH="../../deploy/helpers.sh"
MEMORY="512MB"
TIMEOUT="540"

source "$HELPERS_PATH"
eval "$(parse_yaml $CONFIG_PATH)"

CP=$(cp ../../deploy/gads_config.json ./)


CF_NAME=$CF_NAME_GADS_INVOKER
OUTBOUND_TOPIC_NAME=$STORE_RESPONSE_STATS_TOPIC

PREFIX="$DEPLOYMENT_NAME.$SOLUTION_PREFIX"
OUTBOUND_TOPIC_NAME="$PREFIX.$OUTBOUND_TOPIC_NAME"

create_pubsub_topic "$OUTBOUND_TOPIC_NAME"

CFG_FILE=$(cat $CONFIG_PATH $CUSTOM_CONFIG_PATH > ./__config.yaml)

gcloud functions deploy "$DEPLOYMENT_NAME""_""$SOLUTION_PREFIX""_""$CF_NAME""" \
   --runtime python37 \
   --entry-point gads_invoker \
   --trigger-http \
   --memory "$MEMORY" \
   --timeout "$TIMEOUT" \
   --project "$DEFAULT_GCP_PROJECT" \
   --region "$DEFAULT_GCP_REGION" \
   --service-account "$SERVICE_ACCOUNT" \
   --env-vars-file ./__config.yaml \
   --no-allow-unauthenticated \
   --format "none"


rm ./__config.yaml



