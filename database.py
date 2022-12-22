import textwrap
from typing import Dict, List
import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
import logging

from static import Reddit


def conversion_field(name: str) -> bigquery.SchemaField:
    fields = [
        bigquery.SchemaField("attribution_window_day", "INTEGER"),
        bigquery.SchemaField("attribution_window_month", "INTEGER"),
        bigquery.SchemaField("attribution_window_week", "INTEGER"),
    ]
    click = bigquery.SchemaField("click_through_conversions", "RECORD", fields=fields)
    view = bigquery.SchemaField("view_through_conversions", "RECORD", fields=fields)
    return bigquery.SchemaField(name, "RECORD", fields=(click, view))


def schema_to_bq(schema: Dict[str, str]) -> List[bigquery.SchemaField]:
    """This function converts records into BigQuery compatible datatypes.

    Args:
        schema (Dict[str, str]): The source schema

    Returns:
        List[bigquery.SchemaField]: _description_
    """
    type_dict = {
        "int": "INTEGER",
        "float": "FLOAT",
        "timestamp": "TIMESTAMP",
        "str": "STRING",
    }
    fields: List[bigquery.SchemaField] = []

    for name, dtype in schema.items():
        if dtype.endswith("!"):
            dtype = dtype[:-1]
            mode = "REQUIRED"
        else:
            mode = "NULLABLE"

        if dtype in type_dict:
            fields.append(bigquery.SchemaField(name, type_dict[dtype], mode=mode))
        elif dtype == "conversion":
            fields.append(conversion_field(name))
        else:
            raise ValueError(f"Field type {dtype} is not supported.")

    return fields


class BigQueryDatabase:
    REDDITADS_SCHEMA = dict(
        spend="int",
        video_watched_50_percent="int",
        video_watched_75_percent="int",
        ecpm="float",
        video_viewable_impressions="int",
        date="timestamp",
        ctr="float",
        impressions="int",
        video_watched_3_seconds="int",
        cpc="float",
        video_watched_100_percent="int",
        video_started="int",
        video_plays_with_sound="int",
        ad_id="str",
        clicks="int",
        video_fully_viewable_impressions="int",
        video_plays_expanded="int",
        video_watched_95_percent="int",
        video_watched_10_seconds="int",
        video_watched_25_percent="int",
        page_visit="conversion",
        view_content="conversion",
        search="conversion",
        add_to_cart="conversion",
        add_to_wishlist="conversion",
        purchase="conversion",
        lead="conversion",
        sign_up="conversion",
        custom="conversion",
        ad_group_id="str",
        ad_name="str",
        account_id="str",
        campaign_id="str",
        ad_group_name="str",
        campaign_name="str",
    )

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(
            project=project_id,
            default_query_job_config=bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.BATCH
            ),
        )

    def ensure_table(self, schema: Dict[str, str], table_id: str) -> None:
        """

        Function to ensure if table is present or else
        create it.

        Args:
            schema (Dict[str, str]): The schema of the destination table.
            table_id (str): The name of destination table.

        """
        table = bigquery.Table(table_id, schema=schema_to_bq(schema))
        self.client.create_table(table, exists_ok=True)

    def save_table(self, table: pd.DataFrame) -> None:
        """
        Function to save data into destination table.

        Args:
            table (pd.DataFrame): The dataframe with table's data.
        """
        tmp_table_id = f"{self.project_id}.{Reddit.DATASET}.{Reddit.TMP_TABLE}"
        self.save_new_table(table, tmp_table_id)

        try:
            self.merge_tables(tmp_table_id)
        finally:
            self.client.delete_table(tmp_table_id)

    def merge_tables(self, tmp_table_id: str) -> None:
        """
        This function merges the tmp table with
        the destination table.

        Args:
            tmp_table_id (str): the name of temporary table.
        """
        table_id = f"{self.project_id}.{Reddit.DATASET}.{Reddit.TABLE}"
        self.ensure_table(self.REDDITADS_SCHEMA, table_id)

        insert_cols_list = set(self.REDDITADS_SCHEMA.keys())
        keys = {"date", "ad_id", "ad_group_id", "account_id", "campaign_id"}
        update_cols_list = insert_cols_list - keys

        update_cols = ",\n".join(f"{c} = tmp.{c}" for c in update_cols_list)
        insert_cols = ",\n".join(insert_cols_list)
        value_cols = ",\n".join(f"tmp.{c}" for c in insert_cols_list)
        join_cols = "\nand ".join(f"old.{c} = tmp.{c}" for c in keys)

        def indent(text: str) -> str:
            return textwrap.indent(text, " " * 8)

        query = f"""
        merge {table_id} as old
        using {tmp_table_id} as tmp
            on {indent(join_cols)}
        when matched then
            update set
                {indent(update_cols)}
        when not matched then
            insert (
                {indent(insert_cols)}
            )
            values (
                {indent(value_cols)}
            )
        """
        job = self.client.query(query)
        try:
            job_result = job.result()
            logging.info("Merged %s records", job.num_dml_affected_rows)
        except BadRequest as exc:
            raise ValueError(job.errors) from exc

    def save_new_table(self, table: pd.DataFrame, table_id: str) -> str:
        job_config = bigquery.LoadJobConfig(
            schema=schema_to_bq(self.REDDITADS_SCHEMA),
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        )
        table = table[self.REDDITADS_SCHEMA.keys()]
        job = self.client.load_table_from_dataframe(
            table, table_id, job_config=job_config
        )
        try:
            job.result()
            logging.info("Added %s new records in tmp table", len(table))
        except BadRequest as exc:
            raise ValueError(job.errors) from exc

        return table_id
