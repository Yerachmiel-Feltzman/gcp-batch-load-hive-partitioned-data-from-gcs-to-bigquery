#!/bin/bash
set -e # exit on first error encountered

GCP_MULTI_REGION='us'
GCP_REGION='us-central1'
GCP_PROJECT='my-project'

CLOUD_RUN_SERVICE_NAME='gcp-batch-load-hive-partitioned-data-from-gcs-to-bigquery'
WORKFLOW_NAME='bigquery-ingestion-per-hour-partition-workflow'
WORKFLOW_FULL_PATH="projects/$GCP_PROJECT/locations/$GCP_REGION/workflows/$WORKFLOW_NAME"
SCHEDULER_NAME='bigquery-ingestion-schedule'

WORKFLOW_DEFAULT_ARGUMENTS='{"bucket_name": "my-bucket",
                     "dataset_id": "prod_landing_zone",
                     "table_id": "load_per_hourly_partition"}'

gcp:bigquery:create_landing_table() {
  bq query \
    --use_legacy_sql=false \
    --format="prettyjson" \
    --location="$GCP_MULTI_REGION" \
    --quiet=true \
    --synchronous_mode=true \
    --batch=false \
    "$(cat ./bq_create_table_ddl.sql)"
}

gcp:cloud_run:deploy() {
  gcloud run deploy "$CLOUD_RUN_SERVICE_NAME" \
    --service-account "my-service-account@my-project.iam.gserviceaccount.com" \
    --source . \
    --max-instances 100 \
    --no-allow-unauthenticated \
    --region "$GCP_REGION" \
    --project "$GCP_PROJECT"
}

gcp:cloud_run:get_url() {
  gcloud run services describe "$CLOUD_RUN_SERVICE_NAME" \
    --region "$GCP_REGION" \
    --project "$GCP_PROJECT" \
    --format='value(status.url)'
}

gcp:workflows:deploy() {
  out="$(mktemp)"
  sed "s,__BASE_URL__,$(gcp:cloud_run:get_url),g" workflow.yaml >"$out"
  cat "$out"

  gcloud workflows deploy "$WORKFLOW_NAME" \
    --source="$out" \
    --location "$GCP_REGION" \
    --project "$GCP_PROJECT"
}

gcp:workflow:run:partition() {
  partition="$1"
  arguments="$(echo "$WORKFLOW_DEFAULT_ARGUMENTS" | jq --arg partition "$partition" '. += {"partition": $partition}')"

  gcloud workflows run \
    "$WORKFLOW_FULL_PATH" \
    --location="$GCP_REGION" \
    --data="$(echo "$arguments" | jq)"
}

gcp:scheduler:deploy() {
  scheduler_input="{\"argument\": $(echo "$WORKFLOW_DEFAULT_ARGUMENTS" | jq @json)}"

  gcloud scheduler jobs update http "$SCHEDULER_NAME" \
    --oauth-service-account-email="bigquery-ingestion-scheduler@$GCP_PROJECT.iam.gserviceaccount.com" \
    --location="$GCP_REGION" \
    --time-zone="Etc/UTC" \
    --schedule="40 * * * *" \
    --uri="https://workflowexecutions.googleapis.com/v1/$WORKFLOW_FULL_PATH/executions" \
    --http-method="POST" \
    --attempt-deadline 540s \
    --max-retry-attempts 3 \
    --message-body="$scheduler_input"

}

gcp:all:deploy(){
  gcp:bigquery:create_landing_table
  sleep 5
  gcp:cloud_run:deploy
  sleep 5
  gcp:workflows:deploy
  sleep 5
  gcp:scheduler:deploy
}


# to allow calling function form terminal
# based on https://stackoverflow.com/a/16159057/12894926
"$@"
