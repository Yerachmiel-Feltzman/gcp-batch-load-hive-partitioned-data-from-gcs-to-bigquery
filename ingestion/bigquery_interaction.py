import logging
from typing import NamedTuple, Optional

from google.cloud import bigquery

from ingestion.types import JobState, LoadJobMetadata

logger = logging.getLogger(__name__)


class BigQueryJobNotFound(RuntimeError):
    pass


# TODO: for now using never create so I can control better the table creation
# TODO: add a create if not exists in the beginning of the workflow
replace_partition_csv_job_config = dict(
    create_disposition=bigquery.job.CreateDisposition.CREATE_NEVER,
    # TODO truncate only partitions
    write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    source_format="CSV",
    field_delimiter='\t',
    skip_leading_rows=str(0),
    quote_character='',
    encoding='UTF-8'
)


def start_load_job(bq: bigquery.Client, load_job_metadata: LoadJobMetadata) -> JobState:
    if load_job_metadata.files is None or len(load_job_metadata.files) == 0:
        logger.info(f"No files in job. Skipping job: {load_job_metadata.job_id}")
        return JobState.NOT_CREATED

    # noinspection PyProtectedMember
    bq_job_ref = bigquery.job._JobReference(job_id=load_job_metadata.job_id,
                                            project=load_job_metadata.project_id,
                                            location=load_job_metadata.region)

    table_ref = bigquery.TableReference.from_api_repr({"projectId": load_job_metadata.project_id,
                                                       "datasetId": load_job_metadata.dataset_id,
                                                       "tableId": load_job_metadata.table_id})

    job_config = bigquery.job.LoadJobConfig(**load_job_metadata.job_config)

    # noinspection PyTypeChecker
    bq_job = bigquery.job.LoadJob(job_id=bq_job_ref,
                                  source_uris=load_job_metadata.files,
                                  destination=table_ref,
                                  client=bq,
                                  job_config=job_config)
    try:
        # noinspection PyProtectedMember
        bq_job._begin()
        logger.info(f"Started job {load_job_metadata.job_id}.")
        # noinspection PyProtectedMember
        logger.debug(f"Job information: {bq_job._properties}")
        return JobState.RUNNING

    except Exception as e:
        if bq_job.running():
            logger.info(f"Tried starting a job that already begun: {load_job_metadata.job_id}. "
                        f"Returned job state from bigquery client: {bq_job.state}")
            return JobState.RUNNING

        if bq_job.error_result is not None:
            logger.error(f"Job id {load_job_metadata.job_id} failed with error: {bq_job.error_result}")
            return JobState.FAILURE

        if bq_job.done():
            logger.info(f"Tried to start job {load_job_metadata.job_id}, but it's already done successfully.")
            return JobState.SUCCESS

        else:
            logger.error(f"Failed to begin job {load_job_metadata.job_id} with error: {e}")
            return JobState.FAILURE


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
    # noinspection PyProtectedMember
    bq_job_ref = bigquery.job._JobReference(job_id=job_id,
                                            project=project_id,
                                            location=region)

    # noinspection PyTypeChecker
    bq_job = bigquery.job.LoadJob(job_id=bq_job_ref,
                                  client=bq,
                                  # we can nullify bellow since we won't be running it from here
                                  source_uris=None,
                                  destination=None)

    base_msg = f"Polling job {job_id}. Result: "
    if not bq_job.exists():
        msg = base_msg + f"Job does not exist."
        logger.error(msg)
        raise BigQueryJobNotFound(msg)

    if bq_job.running():
        logger.info(base_msg + f"Job is running.")
        return JobState.RUNNING, None

    if bq_job.error_result is not None:
        logger.error(base_msg + f"Job failed with error: {bq_job.error_result}")
        return JobState.FAILURE, str(bq_job.error_result)

    if bq_job.done():
        logger.info(base_msg + "Job succeeded.")
        return JobState.SUCCESS, None

    else:
        raise RuntimeError(base_msg + f"Unable to poll job. Job state returned by client = {bq_job.state}.")
