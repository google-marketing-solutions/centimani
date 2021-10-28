# Centimani - User & Developer Guide

## What’s Centimani?

Centimani is a configurable massive file processor able to split text files in chunks, process them following a strategic pattern and store the results in BigQuery for reporting. It provides configurable options for chunk size, number of retries and takes care of exponential backoff to ensure all requests have enough retries to overcome potential temporary issues or errors.

In its default version, Centimani comes with two main features (or operators):

- Google Ads offline conversions upload using Google Ads API
- Merchant Center product upload using Content API

The solution can be easily extended, you can find all the details [here](INTERNALS.md#how-to-extend-the-solution?).

## How does it work?

Centimani uses **Cloud Tasks** to parallelize up to thousands of API calls with small payloads, reducing the processing time for big input files. It uses **Secret Manager** to store all sensitive credentials and Datastore to keep an updated report on the progress of the data processing workflow. Finally, reporting status information is sent to **BigQuery**, so it can be analyzed and/or visualized.

![Centimani Workflow](docs/resources/workflow.png "Centimani Workflow")

From a user's perspective, this will be the workflow the tool follows for each execution:

- A file is uplodaded to the input bucket in **Cloud Storage** (information about file syntax and formats [here](#input-files-naming-convention-and-formats)).
- A **Cloud Function** is triggered, and the file is categorized to be processed by one of the operators,
  and is splitted in smaller files.
- A series of tasks are created in **Cloud Tasks** to be launched in parallel in a controlled manner. These tasks
  call the specific function for the selected operator.
- The results of each call are stored in Datastore, and then transfered to BigQuery for reporting.

Everything Centimani does can be audited. As the information from all the operations is
stored in BigQuery, a dashboard can be created to monitor the operations and see the error
details in case any of them failed. You can see a couple of examples below.

![Main Status Dashboard](docs/resources/image4.png "Main Status Dashboard")

![Detailed Errors Report](docs/resources/image5.png "Detailed Errors Report")

## How to deploy?

### Preparation Steps

1. Edit the `config.yaml` file inside the `deploy` directory and configure your solution and cloud project settings, using the first part of the file (up to the ‘DO NOT MODIFY’ line).

   `DEPLOYMENT_NAME` & `SOLUTION_PREFIX`: These values will be used to build the name of every artifact, so no collision with other deployments may happen.

   ```text
   DEPLOYMENT_NAME: 'deployment-name' (i.e: production)
   SOLUTION_PREFIX: 'descriptive-prefix' (i.e: conversion_uploader)
   ```

   `SERVICE_ACCOUNT`: The service account to use. It will be created if it does not exist. Add it without domain if you’re not sure of the domain name. It must be 6 to 30 char long.

   ```text
   SERVICE_ACCOUNT: 'service-account-name-without-domain' (i.e mcu-123)
   ```

   `DEFAULT_GCP_PROJECT` & `DEFAULT_GCP_REGION`: The name of your project and the corresponding region.

   ```text
   DEFAULT_GCP_PROJECT: 'your-project' (i.e massive-conversion-uploader)
   DEFAULT_GCP_REGION: 'your-project-region' (allowed values here)
   ```

   `INPUT_GCS_BUCKET` & `OUTPUT_GCS_BUCKET`: The cloud storage buckets to receive input files and store results.

   ```text
   INPUT_GCS_BUCKET: 'the-input-bucket-name'
   OUTPUT_GCS_BUCKET: 'the-output-bucket-name'
   ```

   `SKIP_DATABASE_CREATION`: Boolean defining if the dataset should be created at deployment time if missing.

   ```text
   SKIP_DATASET_CREATION: 'N'
   ```

   `BQ_REPORTING_DATASET`, `BQ_REPORTING_TABLE` & `BQ_GCP_BROAD_REGION`: The name of the BQ dataset, reporting table and the region where the BQ data will be stored.

   ```text
   BQ_REPORTING_DATASET: 'the-dataset-name'
   BQ_REPORTING_TABLE: 'the-table-name'
   BQ_GCP_BROAD_REGION: 'EU' (allowed values here)
   ```

   `TIMEZONE` & `REPORTING_DATA_POLLING_CONFIG`: Defines the timezone and scheduling (in crontab format) for the reporting stats.

   ```text
   TIMEZONE: 'Europe/Madrid' (allowed values here)
   REPORTING_DATA_POLLING_CONFIG: '\*/5 \* \* \* \*'
   ```

2. Add particular settings for **Google Ads Offline Conversions Uploader**:

   1. Add the Google Ads credentials in the `gads_config.json` file, inside the `deploy` directory.
   2. Edit and replace the following variables in the `gads_config.json` file: *(Replace XXXXX with the customer id you want to login as. This value is used for looking up the conversion action ids)*
      - `credentials.XXXXX.developer_token:` the GAds developer token
      - `credentials.XXXXX.client_id:` the OAUTH2 client id
      - `credentials.XXXXX.client_secret:` the OAUTH2 client secret
      - `credentials.XXXXX.refresh_token:` the OAUTH2 client refresh token
      - `credentials.XXXXX.login_customer_id:` the id of the customer to login as. It has to be the same value as XXXXX

   3. Edit (if required) the performance variables `gads_config.json` file:
      - `slicer.max_chunk_lines:` the number of lines on each file chunk excluding the header (default 2000)
      - `queue_config.name:` the name of the queue for the gads operator (default gads-conversions-upload)
      - `queue_config.max_dispatches_per_second:` number of request to gads operator per second (default 100)
      - `queue_config.max_concurrent_dispatches:` max number of tasks in parallel (default 2000)
      - `queue_config.retry_config.max_attempts:` max retry attempts (default 5)
      - `queue_config.retry_config.max_retry_duration:` max retry duration (default 1750)
      - `queue_config.retry_config.min_backoff:` lower bound in seconds for exponential backoff (default 10)
      - `queue_config.retry_config.max_backoff:` upper bound in seconds for exponential backoff (default 300)
      - `queue_config.retry_config.max_doublings:` how many times the time between retries will be doubled (default 3)

   4. When configuring the settings, please take these tips into consideration:

    - For Google Ads conversions uploading, take into account that the maximum number of conversions in each upload is currently 2,000 but the timeout is currently set to 60 seconds, so we recommend to send a lower number of conversions in each request (around 50), until this discrepancy is fixed.
    - Service account names (the part before the @) must be between 6 and 30 chars long. If it’s a new service account, please don’t include the domain. Otherwise, if you are reusing a service account formerly created, then you should include it.
    - Input and output GCS buckets will be automatically created if they don’t exist already. Please take into account that bucket names are unique across Google, so you may choose a name already in use in another project. If this is the case, you will need to choose a different name for the deployment to succeed.
    - Cloud Function names are limited to 63 chars max. Please do not choose a long deployment name and solution prefix, or the deployment will fail.
    - During the deployment of the schedulers, you may be asked to deploy an App Engine. If that’s the case, just say “Y”es and choose the same location that you used in the configuration file. This App Engine will be used to provide the Cloud Task Scheduler functionality.
    - Delete the file \*_config.json after completing the deployment, in order to avoid making sensitive information available in your deployment system.

3. Add particular settings for **Merchant Center Products Uploader**:

   1. If you already have a service account available in your Google Cloud proyect, download the API key in json format, and include it in the credential section of the `mc_config.json` file, inside the `deploy` directory. If you haven't created the service account yet, the deployment script will do that for you. After the deployment is done, return to this step and add it to the `mc_config.json`file.
   2. Authorize your service account to manage your Merchant Center account. You can do this both at individual level, or at MCA level. If you haven't created the service account yet, do this step after the deployment is done.
   3. Repeat steps 1 and 2 if you want to create separated access for different accounts within the same tool (you can create different credentials in the file, and select which one to use at runtime).

### Deployment

Centimani uses a centralized deployment script which will deploy each of the components. In order to start the process,
once the configuration file is ready, just run the deployment script using its relative path from the root folder of Centimani:

```sh
cd deploy/
bash deploy.sh
```

The deployment script will grant permissions, enable services and create all Google Cloud resources required for
Centimani to run.

Progress in displayed on the screen, so please take a look for any issues reported during the
deployment process. You may re-run the script after fixing any reported issue in order to ensure a healthy installation of the solution.

### Updating the Deployment

If you have already deployed the solution but only want to update the configuration or the cloud functions,
you can use some flags to indicate what needs to be done.
Remember that this is only useful after the tool is fully deployed.

- If you only want to update the configuration, use the `-c` flag:

  ```sh
  cd deploy/
  bash deploy.sh -c
  ```

- If you only want to deploy the cloud functions, use the `-d` flag:
  
  ```sh
  cd deploy/
  bash deploy.sh -f
  ```

## Maintenance

- A dated directory is created everyday in the output bucket, you may want to
  create a rule to delete the older files.
- In case you need to delete datastore entities, you’ll need to create your own
  script to query entities to be deleted and iterate through them.
- In case you need to delete daily BigQuery sharded tables, you can use the
script `_delete_daily_tables.sh_` included in the utils directory


## Troubleshooting

- There are files in the ‘failed’ GCS directory.

  Check for errors in the file name pattern.

- The number of files I uploaded to GCS do not match with the ones in BigQuery.
  
  Check the scheduler and make sure the associated Cloud Function is running correctly.
  
  Check if there are  memory exhaustion errors on reporting_data_extractor cloud function. If so, increase the allocated memory for the Cloud Function.
  
  If you built your own operator, make sure that all errors are handled correctly

- In BigQuery I find more slices and with a different num of rows than than configured.
  
  Have you changed the slicing parameters and reprocessed the file on the same day? If so, it’s normal. Clean the entries for that file in BigQuery and reprocess it.

- File slicing phase takes too long.
  
  Consider uploading smaller files, or creating bigger chunks. (The benchmark we have for GAds conversion upload is 100 input files with 25K records each, split into 50 record files, taking less than 1 hour end to end processing time)
  
  Consider increasing the allocated memory for the Cloud Function to 2GB (you can do it in _cfs/file_slicer/deploy.sh_).

- The operator does not seem to work properly.
  
  Check the Cloud Function logs and make sure that you are not running out of memory (due to the size of the data in Datastore, for example) and the API invocations and returns are working properly.

- I can’t see all of the information from Datastore in BigQuery.
  
  Make sure that the “Report Extractor” Cloud Function is not running out of memory. Otherwise, increase the allocated memory to prevent this situation.

- Why are there separate input and output buckets?
  
  GCS sends finalize events to file_slicer for every object created. This means that using a single bucket would trigger the slicer every time a file slice is written by the slicer or when a file is moved to another directory, generating hundreds of thousands of unnecessary (and charged beyond the free quota) calls. Separating the logic into 2 buckets resolves the unnecessary calls.

## Additional Reference

### Input Files Naming Convention and Formats

Each operator has its own specific syntax to handle filenames. The only part
that is fixed is the initial prefix, that indicates which operator is going to
be used. Also, underscores characters are used as field separators.

For the standard operators included with this solution, the values are:

- `GADS`: Google Ads Offline Conversions Uploader
- `MC`: Merchant Center Products Uploader

You can find below the specific syntax used by each of these operators.

#### Google Ads Offline Conversions Uploader

```text
GADS_<free text no underscore allowed>_<CID where conversions are observed>_<MCC where developer token is defined>_<CID where conversions are defined>_<date string in YYYYMMDD format>_<free text>.csv
```

Where:

1. \<free text no underscore allowed>: use these fields to describe the purpose of the file or add any additional information. You may include customer name, conversion name, etc. Please do not use underscores, since they are used as separators.
2. \<CID where conversions are observed>: the account where the cids are captured.
3. \<MCC where developer token is defined>: the MCC account where we could find the developer token to be used.
4. \<CID where conversions are defined>: the account where the conversion actions are defined.
5. \<date string in YYYYMMDD format>: The date in YYYYMMDD format
6. \<free text> any text

All these details are required because the gAds Operator module needs to obtain the conversion action id from the conversion action name, and then upload the conversions at the right level.

#### Merchant Center Products Uploader

```text
MC_<free text no underscore allowed>_<date string in YYYYMMDD format>.json
```

Where:

1. \<free text no underscore allowed>: use these fields to describe the purpose of the file or add any additional information. You may include customer name, store, etc. Please do not use underscores, since they are used as separators.
4. \<Credential's name>: the name of the credentials to use, as defined in the configuration of the tool.
5. \<date string in YYYYMMDD format>: The date in YYYYMMDD format

For example,

```text
MC_MY-SOMETHING_default_20210421_clothes-store.json
```

The file must be in json format, where each line is a self-contained batch item
prepared for the Content API for shopping. This allows the tool to do the split operation more efficiently.

Following is an example of a batch item, expanded to several lines just for visualization:

```json
{
  "batchId":123,
  "merchantId":"12345678",
  "method":"insert",
  "product":{
    "id":"online:es:ES:MY-ID-123",
    "offerId":"MY-ID-123",
    "title":"My Awesome Product","link":"...",
    "price":{"value":10,"currency":"EUR"},
    "description":"...",
    //...
  }
}
```

The expected format for this particular item in the input file should be this (all in a single line):

```json
{ "batchId":123, "merchantId":"12345678", "method":"insert", "product":{ "id":"online:es:ES:MY-ID-123", "offerId":"MY-ID-123", "title":"My Awesome Product","link":"...", "price":{"value":10,"currency":"EUR"}, "description":"...", ... } }
```

The specific format would depend on the type of operation to be performed, but it must
be compliant with the format required by the Content API.
