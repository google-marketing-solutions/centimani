#!/bin/bash

source ../deploy/helpers.sh
eval $(parse_yaml ../deploy/config.yaml)

gcloud config set project $DEFAULT_GCP_PROJECT
gcloud config set compute/region $DEFAULT_GCP_REGION

for i in $(bq ls -n 9999 $1 | grep $2 | awk '{print $1}'); do bq rm $3 -t $1.$i;done;