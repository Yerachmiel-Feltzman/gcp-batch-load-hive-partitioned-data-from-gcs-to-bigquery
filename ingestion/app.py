import datetime
import logging
import os

from fastapi import FastAPI, status, HTTPException
from google.cloud import bigquery, storage
from pydantic import BaseModel

from ingestion import tasks, bigquery_interaction, config
from ingestion.partition import bq_partition_by_hour_from_datetime, \
    partition_datetime_from_bq_partition, hive_partition_by_hour_path_from_bq_partition, gcs_partition_path_glob_all

config.set_log_level_from_env(force=False)
logger = logging.getLogger(__name__)

default_project_id = os.environ.get("GCP_PROJECT", "my-project")
default_region = os.environ.get("GCP_REGION", "us")

bq_client = bigquery.Client(project=default_project_id)
gsc_client = storage.Client(project=default_project_id)

# TODO: async?
# TODO: can I validate partition with pydantic?


app = FastAPI()


class NewLoadJob(BaseModel):
    bucket_name: str
    dataset_id: str
    table_id: str
    job_configuration: None | dict[str, str] = bigquery_interaction.replace_partition_csv_job_config


class JobStatus(BaseModel):
    name: str
    code: int
    error_msg: None | str = None


class LoadJob(BaseModel):
    job_id: str
    status: JobStatus


@app.get("/")
def root():
    return "That's the root page of this API."


@app.get("/partition/last_hour/exists/in-bucket", status_code=status.HTTP_200_OK)
def check_last_hour_partition_exists_in_bucket(bucket_name: str):
    part = _last_hour_partition()
    return _partition_exists_in_bucket(partition_bq=part, bucket_name=bucket_name)


@app.put("/partition/last_hour/ingest", status_code=status.HTTP_201_CREATED)
def ingest_last_hour_partition(new_job_info: NewLoadJob,
                               project_id: str = default_project_id,
                               region: str = default_region):
    partition = _last_hour_partition()

    return _create_and_run_for_partition(partition=partition,
                                         new_job_info=new_job_info,
                                         project_id=project_id,
                                         region=region)


@app.get("/partition/{partition}/exists/in-bucket", status_code=status.HTTP_200_OK)
def check_partition_exists_in_bucket(partition: str, bucket_name: str):
    return _partition_exists_in_bucket(partition_bq=partition, bucket_name=bucket_name)


@app.put("/partition/{partition}/ingest", status_code=status.HTTP_201_CREATED)
def ingest_partition(partition: str,
                     new_job_info: NewLoadJob,
                     project_id: str = default_project_id,
                     region: str = default_region):
    return _create_and_run_for_partition(partition=partition,
                                         new_job_info=new_job_info,
                                         project_id=project_id,
                                         region=region)


@app.get("/load_job/{job_id}/status")
def poll_status(job_id: str, project_id: str = default_project_id, region: str = default_region):
    try:
        state, error_msg = tasks.poll_load_job_status(bq=bq_client, job_id=job_id, project_id=project_id, region=region)
        print(type(error_msg))
        return LoadJob(job_id=job_id, status=JobStatus(name=state.name, code=state.value, error_msg=error_msg))

    except bigquery_interaction.BigQueryJobNotFound:
        raise HTTPException(status_code=404, detail=f"Job not found.")


def _partition_exists_in_bucket(bucket_name: str, partition_bq: str):
    partition_path = hive_partition_by_hour_path_from_bq_partition(partition_bq)
    blobs = gsc_client.list_blobs(
        bucket_name,
        prefix=partition_path,
        max_results=1)
    for _ in blobs:
        return 1
    return 0


def _create_and_run_for_partition(partition: str,
                                  new_job_info: NewLoadJob,
                                  project_id: str = default_project_id,
                                  region: str = default_region):
    partition_dt = partition_datetime_from_bq_partition(partition)

    job = tasks.create_and_run_load_job_for_partition(
        bq=bq_client,
        bucket_name=new_job_info.bucket_name,
        project_id=project_id,
        dataset_id=new_job_info.dataset_id,
        table_id=new_job_info.table_id,
        job_config=new_job_info.job_configuration,
        region=region,
        partition=partition_dt
    )

    return LoadJob(job_id=job.job_id, status=JobStatus(name=job.status.name, code=job.status.value))


def _last_hour_partition():
    now = datetime.datetime.utcnow()
    last_hour_dt = now - datetime.timedelta(hours=1)
    partition = bq_partition_by_hour_from_datetime(last_hour_dt)

    logger.info(f"Last hour partition is '{partition}'")
    return partition
