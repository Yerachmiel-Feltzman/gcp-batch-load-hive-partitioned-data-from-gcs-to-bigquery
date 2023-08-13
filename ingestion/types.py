import enum
from typing import NamedTuple, List


class JobState(enum.Enum):
    # Defines states of a jobs
    NOT_CREATED = 0
    RUNNING = 1
    SUCCESS = 2
    FAILURE = 3


class JobType(enum.Enum):
    # Defines types of jobs
    LOAD_JOB = 0
    QUERY_JOB = 1


class LoadJobMetadata(NamedTuple):
    job_id: str
    project_id: str
    dataset_id: str
    table_id: str
    region: str
    status: JobState
    files: List[str]
    job_config: dict[str, any]


