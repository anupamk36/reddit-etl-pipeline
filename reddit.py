import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import click
import pandas as pd
import requests
from requests.exceptions import HTTPError
from ratelimit import limits, sleep_and_retry
from auth import RedditAuth
from database import BigQueryDatabase

from static import Reddit

logging.basicConfig(level=logging.INFO)


class RedditApi:
    def __init__(self) -> None:
        self.base = "https://ads-api.reddit.com/api/v2.0"
        auth = RedditAuth()
        config = auth.config
        self.account_id = config["account_id"]
        self.headers = {
            "Authorization": f"bearer {config['access_token']}",
            "User-Agent": "Turing Data Extraction",
        }

    @sleep_and_retry
    @limits(calls=1, period=1)
    def make_request(
        self,
        resource: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base}/accounts/{self.account_id}/{resource}"
        res = requests.get(
            url, params=params, data=data, headers=self.headers, timeout=10
        )
        res.raise_for_status()
        payload = res.json()

        return payload["data"]

    def get_reports(self, start_date: datetime) -> List[Dict[str, Any]]:
        """Function to generate reports

        Args:
            start_date (datetime): The date since when data is to be
                                   fetched, Default is : "2021-03-01".

        Returns:
            List[Dict[str, Any]]: Contains the generated report.
        """
        starts_at = start_date.strftime(Reddit.BQ_DATE_FORMAT)
        data = {
            "starts_at": starts_at,
            "group_by": ["date", "ad_id"],
        }
        return self.make_request("reports", params=data)

    def get_ad_groups(self) -> List[Dict[str, Any]]:
        """Function to call the 'ad_groups' endpoint

        Returns:
            List[Dict[str, Any]]: Returns the data fetched
            from the ad_groups endpoint.
        """
        return self.make_request("ad_groups")

    def get_ads(self) -> List[Dict[str, Any]]:
        """Function to call the 'ads' endpoint.

        Returns:
            List[Dict[str, Any]]: Returns the data fetched
            from the ads endpoint.
        """
        return self.make_request("ads")

    def get_campaigns(self) -> List[Dict[str, Any]]:
        return self.make_request("campaigns")


def transform_report(report: List[Dict[str, Any]]) -> pd.DataFrame:
    dataframe = (
        pd.DataFrame(report)
        .drop(columns=["ad_group_id", "campaign_id", "account_id"])
        .dropna(axis="columns", how="all")
    )
    dataframe["date"] = pd.to_datetime(dataframe.date)
    return dataframe


def transform_ads(ads: List[Dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.DataFrame(ads)[
        [
            "id",
            "ad_group_id",
            "name",
        ]
    ]
    dataframe = dataframe.rename(columns={"id": "ad_id", "name": "ad_name"})
    return dataframe


def transform_ad_groups(ad_groups: List[Dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.DataFrame(ad_groups)[
        [
            "id",
            "account_id",
            "campaign_id",
            "name",
        ]
    ]
    dataframe = dataframe.rename(
        columns={"id": "ad_group_id", "name": "ad_group_name"})
    return dataframe


def transform_campagins(campaigns: List[Dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.DataFrame(campaigns)[
        [
            "id",
            "name",
        ]
    ]
    dataframe = dataframe.rename(
        columns={"id": "campaign_id", "name": "campaign_name"})
    return dataframe


def get_result(api: RedditApi, start_date: datetime) -> pd.DataFrame:
    report = api.get_reports(start_date)
    ads = api.get_ads()
    ad_groups = api.get_ad_groups()
    campaigns = api.get_campaigns()

    report_df = transform_report(report)
    ads_df = transform_ads(ads)
    ad_groups_df = transform_ad_groups(ad_groups)
    campaigns_df = transform_campagins(campaigns)

    result = pd.merge(report_df, ads_df, on="ad_id")
    result = pd.merge(result, ad_groups_df, on="ad_group_id")
    result = pd.merge(result, campaigns_df, on="campaign_id")
    result = result.sort_values("date")
    return result


@click.command()
@click.option(
    "--start-date",
    default=Reddit.DEFAULT_START_DATE,
    type=click.DateTime(["%Y-%m-%d"]),
    show_default=True,
)
@click.option("--project_id", default="", type=str)
def main(project_id: str, start_date: datetime) -> None:
    db = BigQueryDatabase(project_id=project_id)
    api = RedditApi()

    try:
        logging.info("Fetching data from API. Start date: %s", start_date)
        results = get_result(api, start_date)
        logging.info(f"Uploading to dataset: {Reddit.DATASET}.")
        db.save_table(results)
    except HTTPError as exc:
        logging.exception(exc)


if __name__ == "__main__":
    main()
