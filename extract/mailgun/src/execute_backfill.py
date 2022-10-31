""" Extracts data from the MailGun API event stream """
import datetime
import json
import sys
from email import utils
from logging import info, basicConfig, getLogger, error
from os import environ as env
from typing import Dict, List

import requests
from fire import Fire

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

config_dict = env.copy()

api_key = env.get("MAILGUN_API_KEY")
domains = ["mg.gitlab.com"]


def chunker(seq: List, size: int):
    """

    :param seq:
    :param size:
    :return:
    """
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def get_logs(domain: str, event: str, formatted_date: str) -> requests.Response:
    """
    Small convenience wrapper function for mailgun event requests,
    :param domain:
    :param event:
    :param formatted_date:
    :return:
    """
    return requests.get(
        f"https://api.mailgun.net/v3/{domain}/events",
        auth=("api", api_key),
        params={"begin": formatted_date, "ascending": "yes", "event": event},
    )


def extract_logs(event: str, start_date: datetime.datetime, end_date: datetime.datetime) -> List[Dict]:
    """
    Requests and retrieves the event logs for a particular event.
    :param start_date:
    :param event:
    :return:
    """
    page_token = None
    all_results: List[Dict] = []

    formatted_start_date = utils.format_datetime(start_date)
    formatted_end_date = utils.format_datetime(end_date)
    for domain in domains:
        first_timestamp = formatted_start_date

        while first_timestamp < formatted_end_date:
            if page_token:
                response = requests.get(page_token, auth=("api", api_key))
                try:
                    data = response.json()
                except json.decoder.JSONDecodeError:
                    error("No response received")
                    break

                items = data.get("items")

                if items is None:
                    break

                if len(items) == 0:
                    break

                first_timestamp = items[0].get("timestamp")
                str_stamp = datetime.datetime.fromtimestamp(first_timestamp).strftime(
                    "%d-%m-%Y %H:%M:%S.%f"
                )
                info(f"Processed data starting on {str_stamp}")

                all_results = all_results[:] + items[:]

            else:
                response = get_logs(domain, event, formatted_start_date)

                try:
                    data = response.json()
                except json.decoder.JSONDecodeError:
                    error("No response received")
                    break

                items = data.get("items")

                if items is None:
                    break

                if len(items) == 0:
                    break

                all_results = all_results[:] + items[:]

            page_token = data.get("paging").get("next")

            if not page_token:
                break

    return all_results


def load_event_logs(event: str, start_date: datetime.datetime, end_date: datetime.datetime):
    """
    CLI main function, starting point for setting up engine and processing data.
    :param event:
    :param full_refresh:
    """
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    info(f"Running {event} for {str(start_date)} to {str(end_date)}")

    results = extract_logs(event, start_date, end_date)

    info(f"Results length: {len(results)}")

    # Stay under snowflakes max column size.
    file_count = 0
    for group in chunker(results, 10000):
        file_count = file_count + 1
        file_name = f"{event}_{file_count}.json"

        with open(file_name, "w", encoding="utf-8") as outfile:
            json.dump(group, outfile)

        snowflake_stage_load_copy_remove(
            file_name,
            f"mailgun.mailgun_load_{event}",
            "mailgun.mailgun_events",
            snowflake_engine,
            on_error="ABORT_STATEMENT",
        )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire({"load_event_logs": load_event_logs})
    info("Complete.")