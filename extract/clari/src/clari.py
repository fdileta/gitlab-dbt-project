"""
Based on the DAG `execution_date` and `task_schedule`
derives the fiscal_quarter.

The fiscal_quarter is used to request the `net_arr` Clari endpoint.
There are actually 3 endpoints that need to be called:
1. export endpoint: start the `net_arr` export
2. job status endpoint: poll until the job is 'DONE'
3. results endpoint: returns the report as a json object

The resulting json object is converted to a dataframe and
then uploaded to Snowflake.
"""

import os
import sys
import time
import json

from datetime import datetime
from logging import info, basicConfig, getLogger, error
from typing import Any, Dict, Optional
from dateutil import parser as date_parser

import requests
import pandas as pd

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

config_dict = os.environ.copy()
HEADERS = {"apikey": config_dict.get("CLARI_API_KEY")}
BASE_URL = "https://api.clari.com/v4"


def _calc_fiscal_quarter(date_time: datetime) -> str:
    """Based on datetime object, return it's Gitlab fiscal quarter"""
    fiscal_year = date_time.year + 1
    if date_time.month in [2, 3, 4]:
        fiscal_quarter = 1
    elif date_time.month in [5, 6, 7]:
        fiscal_quarter = 2
    elif date_time.month in [8, 9, 10]:
        fiscal_quarter = 3
    else:
        fiscal_quarter = 4

    # Format the fiscal year and quarter as a string
    fiscal_year_quarter = f"{fiscal_year}_Q{fiscal_quarter}"
    return fiscal_year_quarter


def _get_previous_fiscal_quarter(date_time: datetime) -> str:
    """
    Based on datetime object, return it's Gitlab previous fiscal quarter o

    This function isn't currently used. Instead, DAG will control the date.
    """
    current_fiscal_quarter = _calc_fiscal_quarter(date_time)
    current_quarter_int = int(current_fiscal_quarter[-1])
    current_year_int = int(current_fiscal_quarter[:4])

    if current_quarter_int == 1:
        return f"{current_year_int-1}_Q4"
    return f"{current_year_int}_Q{current_quarter_int - 1}"


def get_fiscal_quarter() -> str:
    """
    Return the fiscal quarter based on the passed in dag 'execution_date'

    The goal is for daily DAG runs, return the current fiscal quarter
    and for quarterly runs, return the previous fiscal quarter

    That logic though is handled within the daily/quarterly DAG's
    """
    execution_date = date_parser.parse(config_dict["execution_date"])
    task_schedule = config_dict["task_schedule"]

    info(
        f"Calculating quarter based on the following task_schedule \
        and execution_date: {task_schedule} | {execution_date}"
    )

    # if task_schedule == "daily":
    return _calc_fiscal_quarter(execution_date)

    # else quarterly task schedule
    # return _get_previous_fiscal_quarter(execution_date)


def make_request(
    request_type: str,
    url: str,
    headers: Optional[Dict[Any, Any]] = None,
    params: Optional[Dict[Any, Any]] = None,
    json: Optional[Dict[Any, Any]] = None,
    timeout: int = 60,
    current_retry_count: int = 0,
    max_retry_count: int = 5,
) -> requests.models.Response:
    """Generic function that handles making GET and POST requests"""
    if current_retry_count >= max_retry_count:
        raise Exception(f"Too many retries when calling the {url}")
    try:
        if request_type == "GET":
            response = requests.get(
                url, headers=headers, params=params, timeout=timeout
            )
        elif request_type == "POST":
            response = requests.post(
                url, headers=headers, json=json, timeout=timeout
            )
        else:
            raise ValueError("Invalid request type")

        response.raise_for_status()
        return response
    except requests.exceptions.RequestException:
        if response.status_code == 429:
            retry_after = int(response.headers["Retry-After"])
            time.sleep(retry_after)
            current_retry_count += 1
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=timeout,
                current_retry_count=current_retry_count,
                max_retry_count=max_retry_count,
            )
        error(f"request exception for url {url}, see below")
        raise


def start_export_report(fiscal_quarter: str) -> str:
    """
    Make POST request to start report export for a specific fiscal_quarter
    """
    forecast_id = "net_arr"
    forecast_url = f"{BASE_URL}/export/forecast/{forecast_id}"

    json = {"timePeriod": fiscal_quarter, "includeHistorical": True}
    response = make_request("POST", forecast_url, HEADERS, json=json)
    return response.json()["jobId"]


def get_job_status(job_id: str) -> str:
    """Returns the status of the job with the specified ID."""
    job_status_url = f"{BASE_URL}/export/jobs/{job_id}"
    response = make_request("GET", job_status_url, HEADERS)
    info(f'\njobStatus response:\n {response.json()["job"]}')
    return response.json()["job"]


def poll_job_status(
    job_id: str, wait_interval_seconds: int = 30, max_poll_attempts: int = 5
) -> bool:
    """
    Polls the API for the status of the job with the specified ID,
    waiting for the specified interval between polls.

    Will either return True, or raise an exception if the poll fails
    """
    poll_attempts = 0
    while True:
        status = get_job_status(job_id)["status"]
        poll_attempts += 1
        info(f"Poll attempt {poll_attempts} current status: {status}")
        if status == "DONE":
            info(
                f"job_id {job_id} successfully completed, \
                it is ready for export."
            )
            return True

        if status in ["ABORTED", "FAILED", "CANCELLED"]:
            raise Exception(
                f"job_id {job_id} failed to complete \
                with {status} status"
            )

        if poll_attempts >= max_poll_attempts:  # (SCHEDULED, STARTED) status
            raise TimeoutError(
                f"Poll attempts to the job status API for \
                job_id {job_id} have exceeded \
                maximum poll attempts, aborting."
            )
        time.sleep(wait_interval_seconds)


def get_report_results(job_id: str) -> Dict[Any, Any]:
    """Get the report results as a json/dict object"""
    results_url = f"{BASE_URL}/export/jobs/{job_id}/results"
    response = make_request("GET", results_url, HEADERS)
    info("Successfully obtained report data")
    return response.json()


def results_dict_to_dataframe(
    results_dict: Dict[Any, Any], fiscal_quarter: str
) -> pd.DataFrame:
    """
    returns a dataframe with the following cols:
        - jsontext
        - api_fiscal_quarter_name
        - dag_schedule
        - uploaded_at
    """
    dataframe = pd.DataFrame(
        columns=["jsontext", "api_fiscal_quarter_name", "dag_schedule"]
    )
    dataframe.loc[0] = [
        json.dumps(results_dict),
        fiscal_quarter.replace("_", "-"),  # match dim table formatting
        config_dict["task_schedule"],
    ]
    return dataframe


def upload_dataframe_to_snowflake(dataframe: pd.DataFrame) -> None:
    """Uploads Clari dataframe to Snowflake"""
    info("Uploading dataframe to Snowflake...")
    loader_engine = snowflake_engine_factory(config_dict, "LOADER")

    dataframe_uploader(
        dataframe,
        loader_engine,
        table_name="net_arr",
        schema="clari",
        add_uploaded_at=True,
    )
    info("Successfully uploaded report data to Snowflake")
    loader_engine.dispose()


def check_valid_quarter(
    original_fiscal_quarter: str, results_dict: Dict[Any, Any]
) -> None:
    """
    Double check that the data returned from the API
    matches the quarter the user is looking for

    This is a good double-check because if the API endpoint does not
    recognize some parameter, it defaults to the current quarter
    which may not be the intention
    """
    api_fiscal_quarter = results_dict["timePeriods"][0]["timePeriodId"]
    if api_fiscal_quarter != original_fiscal_quarter:
        raise ValueError(
            f"The data returned from the API \
        has an api_fiscal_quarter of {api_fiscal_quarter}\n \
        This does not match the original \
        fiscal quarter {original_fiscal_quarter}. \
        Most likely the original quarter has no data. Aborting..."
        )


def main() -> None:
    """Main driver function"""
    fiscal_quarter = get_fiscal_quarter()
    info(f"Processing fiscal_quarter: {fiscal_quarter}")

    job_id = start_export_report(fiscal_quarter)
    poll_job_status(job_id)

    results_dict = get_report_results(job_id)
    check_valid_quarter(fiscal_quarter, results_dict)

    dataframe = results_dict_to_dataframe(results_dict, fiscal_quarter)
    upload_dataframe_to_snowflake(dataframe)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    main()
    info("Complete.")
