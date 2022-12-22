from enum import Enum


class Reddit(str, Enum):
    PROJECT_ID = "702739857141"
    SECRET_STRING = "reddit_secret"
    DATASET = "reddit"
    TABLE = "redditads"
    TMP_TABLE = "redditads_tmp"
    BQ_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    DEFAULT_START_DATE = "2021-03-01"
