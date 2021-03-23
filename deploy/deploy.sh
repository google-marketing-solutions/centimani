#!/bin/bash
# Performs the BQ elements creation
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

ENV_PATH="./env.sh"
FUNCTIONS_DIR="../cfs"

source "$ENV_PATH"

function deploy_solution {

  print_welcome_message
  gcloud config set project "$DEFAULT_GCP_PROJECT"
  gcloud config set compute/region "$DEFAULT_GCP_REGION"

  enable_services
  echo "**************************************************************"
  echo "* Services Enabled. Waiting for changes to be applied...     *"
  echo "**************************************************************"
  sleep 30
  create_service_account
  sleep 30
  echo "**************************************************************"
  echo "* Service Account Created.                                   *"
  echo "**************************************************************"
  set_service_account_permissions
  echo "**************************************************************"
  echo "* Account Permissions Set.                                   *"
  echo "**************************************************************"
  create_secrets
  echo "**************************************************************"
  echo "* Secrets Created.                                            *"
  echo "**************************************************************"
  deploy_cloud_functions
  echo "**************************************************************"
  echo "* Cloud Functions Successfully Deployed.                     *"
  echo "**************************************************************"
  create_schedulers
  echo "**************************************************************"
  echo "* Schedulers Successfully Deployed.                          *"
  echo "**************************************************************"
  create_bq_dataset
  echo "**************************************************************"
  echo "**************************************************************"
  echo " IMPORTANT: run the post deployment tasks explained in the doc!"
  echo " IMPORTANT: grant $SERVICE_ACCOUNT on external resources!!    "
  echo "**************************************************************"
  echo "**************************************************************"
  print_completion_message
}
# Prints the ASCII-art logo
function print_logo {
  echo "
  ███╗   ███╗ █████╗ ███████╗███████╗██╗██╗   ██╗███████╗
  ████╗ ████║██╔══██╗██╔════╝██╔════╝██║██║   ██║██╔════╝
  ██╔████╔██║███████║███████╗███████╗██║██║   ██║█████╗
  ██║╚██╔╝██║██╔══██║╚════██║╚════██║██║╚██╗ ██╔╝██╔══╝
  ██║ ╚═╝ ██║██║  ██║███████║███████║██║ ╚████╔╝ ███████╗
  ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═══╝  ╚══════╝

   ██████╗ ██████╗ ███╗   ██╗██╗   ██╗███████╗██████╗ ███████╗██╗ ██████╗ ███╗   ██╗███████╗
  ██╔════╝██╔═══██╗████╗  ██║██║   ██║██╔════╝██╔══██╗██╔════╝██║██╔═══██╗████╗  ██║██╔════╝
  ██║     ██║   ██║██╔██╗ ██║██║   ██║█████╗  ██████╔╝███████╗██║██║   ██║██╔██╗ ██║███████╗
  ██║     ██║   ██║██║╚██╗██║╚██╗ ██╔╝██╔══╝  ██╔══██╗╚════██║██║██║   ██║██║╚██╗██║╚════██║
  ╚██████╗╚██████╔╝██║ ╚████║ ╚████╔╝ ███████╗██║  ██║███████║██║╚██████╔╝██║ ╚████║███████║
  ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝

  ██╗   ██╗██████╗ ██╗      ██████╗  █████╗ ██████╗ ███████╗██████╗
  ██║   ██║██╔══██╗██║     ██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
  ██║   ██║██████╔╝██║     ██║   ██║███████║██║  ██║█████╗  ██████╔╝
  ██║   ██║██╔═══╝ ██║     ██║   ██║██╔══██║██║  ██║██╔══╝  ██╔══██╗
  ╚██████╔╝██║     ███████╗╚██████╔╝██║  ██║██████╔╝███████╗██║  ██║
   ╚═════╝ ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝

  "

}
# Prints the welcome message before deployment begins.
function print_welcome_message {
  print_logo
}

# Enable the necessary cloud services used
function enable_services {
  gcloud services enable \
    compute.googleapis.com \
    appengine.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    firestore.googleapis.com \
    servicemanagement.googleapis.com \
    servicecontrol.googleapis.com \
    bigquery.googleapis.com \
    cloudtasks.googleapis.com \
    secretmanager.googleapis.com \
    --format "none"
}

function create_secrets {
  FILES=*_config.json
  for f in $FILES
  do
     echo "Creating secrets for config file $f..."
     TMP_PLATFORM=$(echo $f | cut -d "_" -f 1)
     SECRET_ID="${DEPLOYMENT_NAME}_${SOLUTION_PREFIX}_${TMP_PLATFORM}_config"
     gcloud secrets create $SECRET_ID \
       --replication-policy=user-managed \
       --locations=${DEFAULT_GCP_REGION} \
       --verbosity=none

     gcloud secrets versions add $SECRET_ID --data-file="${f}"
  done
}

# Loop through the CF directories and deploy the Cloud Functions
function deploy_cloud_functions {
  for cf_dir in $FUNCTIONS_DIR/*/
  do
    pushd "$cf_dir" > /dev/null
    echo "Now deploying $cf_dir"
    sh deploy.sh
    popd > /dev/null
  done
}
function create_schedulers {
    PREFIX="$DEPLOYMENT_NAME.$SOLUTION_PREFIX"

    OUTBOUND_TOPIC_NAME="$PREFIX.$REPORTING_DATA_EXTRACTOR_TOPIC"

    create_pubsub_topic "$OUTBOUND_TOPIC_NAME"

    SC1=$(gcloud scheduler jobs create pubsub "$DEPLOYMENT_NAME""_""$SOLUTION_PREFIX""_reporting_data" \
    --schedule "${REPORTING_DATA_POLLING_CONFIG//\\/}" \
    --topic "$OUTBOUND_TOPIC_NAME" \
    --time-zone "${TIMEZONE//\\/}" \
    --message-body "It's Reporting Time!" \
    --format "none" \
    --verbosity=none
    )

    echo "$SC1"
    ERROR=$(echo "$SC1" | grep -Po "error")
    echo "$ERROR"

    if [[ "$ERROR" == "error" ]]; then
      EXISTS=$(echo "$SC1" |  grep -Po "exists")
      if [[ "$EXISTS" == "exists" ]]; then
          echo "INFO: Stopper scheduler already exists."
      else
          echo "$SC1"
          exit -1
      fi
    fi
}
# Determines if the specified service account exists.
# If not, creates a new service account and gives it some necessary permissions.
function create_service_account {
  # Check whether an existing Gaudi service account exists
  CREATE_SERVICE_ACCOUNT=$(gcloud iam service-accounts list \
    --filter="email ~ $SERVICE_ACCOUNT" \
    --format="value(email)")
  echo "First Check: $CREATE_SERVICE_ACCOUNT"
  # If the client service account doesn't exist
  if [[ -z "$CREATE_SERVICE_ACCOUNT" ]] || [[ $CREATE_SERVICE_ACCOUNT == "" ]]
  then
    CLEAN_SERVICE_ACCOUNT=$(echo "$SERVICE_ACCOUNT" | sed 's/@.*//')
    echo "$CLEAN_SERVICE_ACCOUNT"
    RESULT_CREATE_SERVICE_ACCOUNT=$(gcloud iam service-accounts create "$CLEAN_SERVICE_ACCOUNT" \
      --description "Service Account" \
      --display-name "Service Account" \
      --format="value(email)")
    echo "$RESULT_CREATE_SERVICE_ACCOUNT"
  fi
  if [[ "$SERVICE_ACCOUNT" = *"@"* ]]; then
      echo "Service Account Contains The Domain Already. Skipping..."
  else
      sed -i "s/SERVICE_ACCOUNT.*/SERVICE_ACCOUNT: '$CREATE_SERVICE_ACCOUNT'/" ./config.yaml
      echo "Domain Added To Service Account."
  fi
}
function set_service_account_permissions {
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/editor \
  --format "none"
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/bigquery.admin \
  --format "none"
  gcloud projects add-iam-policy-binding "$DEFAULT_GCP_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/secretmanager.secretAccessor \
  --format "none"

}

function create_bq_dataset {
  if [[ "$SKIP_DATASET_CREATION" == "N" ]]; then
    CREATE_DATASET=$(bq mk -d \
    --location "$BQ_GCP_BROAD_REGION" \
    "$DEFAULT_GCP_PROJECT:$BQ_REPORTING_DATASET"
    )
    ERROR=$(echo "$CREATE_DATASET" | grep -Po "error")
    #echo $ERROR
    if [[ "$ERROR" == "error" ]]; then
      EXISTS=$(echo "$CREATE_DATASET" | grep -Po "exists")
      if [[ "$EXISTS" == "exists" ]]; then
          echo "INFO: Dataset $BQ_REPORTING_DATASET Already exists."
      else
          echo "$CREATE_DATASET"
          exit -1
      fi
    else
      echo "***************************************************************"
      echo "* BQ Dataset Successfully Deployed. Waiting to be available.  *"
      echo "***************************************************************"
      sleep 30
    fi
  fi
}

# Print the completion message once all components have been deployed.
function print_completion_message {
  echo "
<><><><><><><><><><><><><><><><><><><>
"
  echo "$DEPLOY_NAME.$SOLUTION_NAME Massive Conversions Uploader has now been deployed.
Please check the logs for any errors. If there are none, you're all set up!
"
}
deploy_solution
