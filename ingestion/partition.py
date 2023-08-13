import datetime

HIVE_PARTITION_BY_HOUR_FORMAT = "year=%Y/month=%m/day=%d/hour=%H"
BQ_PARTITION_BY_HOUR_FORMAT = "%Y%m%d%H"


def bq_partition_by_hour_from_datetime(partition_dt: datetime.datetime) -> str:
    return partition_dt.strftime(BQ_PARTITION_BY_HOUR_FORMAT)


def partition_datetime_from_bq_partition(partition_bq: str) -> datetime.datetime:
    return datetime.datetime.strptime(partition_bq, BQ_PARTITION_BY_HOUR_FORMAT)


def hive_partition_by_hour_path_from_datetime(partition_dt: datetime.datetime) -> str:
    return partition_dt.strftime(HIVE_PARTITION_BY_HOUR_FORMAT)


def hive_partition_by_hour_path_from_bq_partition(partition_bq: str) -> str:
    partition_dt = partition_datetime_from_bq_partition(partition_bq)
    partition_path = hive_partition_by_hour_path_from_datetime(partition_dt)
    return partition_path


def gcs_partition_path_glob_all(bucket_name: str, hive_partition_path: str):
    path = hive_partition_path.removesuffix("/")
    return "gs://%s/%s/*" % (bucket_name, path)
