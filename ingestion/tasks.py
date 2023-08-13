import datetime
import logging
import uuid
from typing import Optional

from google.cloud import bigquery

from ingestion import bigquery_interaction
from ingestion.types import JobState, LoadJobMetadata
from ingestion.partition import bq_partition_by_hour_from_datetime, \
    hive_partition_by_hour_path_from_datetime, gcs_partition_path_glob_all

logger = logging.getLogger(__name__)


def create_and_run_load_job_for_partition(bq: bigquery.Client,
                                          bucket_name: str,
                                          project_id: str,
                                          dataset_id: str,
                                          table_id: str,
                                          region: str,
                                          job_config: dict[str, any],
                                          partition: datetime.datetime) -> Optional[LoadJobMetadata]:
    table_partition_suffix = bq_partition_by_hour_from_datetime(partition)
    table_id_with_partition = table_id + "$" + table_partition_suffix

    hive_partition_path = hive_partition_by_hour_path_from_datetime(partition)
    gcs_partition_path = gcs_partition_path_glob_all(bucket_name, hive_partition_path)

    job_id = str(uuid.uuid4())

    new_job_metadata = LoadJobMetadata(
        job_id=job_id,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id_with_partition,
        region=region,
        status=JobState.NOT_CREATED,
        files=[gcs_partition_path],
        job_config=job_config
    )

    load_job_status: JobState = bigquery_interaction.start_load_job(bq, new_job_metadata)
    return new_job_metadata._replace(status=load_job_status)


def poll_load_job_status(bq: bigquery.Client,
                         job_id: str,
                         project_id: str,
                         region: str) -> (JobState, Optional[str]):
    """

    :param bq:
    :param job_id:
    :param project_id:
    :param region:
    :return: Tuple of: job status, error message (is status==failed, otherwise return None)
    """
    status, error_msg = bigquery_interaction.poll_load_job_status(bq=bq,
                                                                  job_id=job_id,
                                                                  project_id=project_id,
                                                                  region=region)

    return status, error_msg
