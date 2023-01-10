import logging
import os
import csv

from typing import Dict

from dateutil import rrule
from datetime import datetime, timedelta

from gitlabdata.orchestration_utils import (
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
    bizible_snowflake_engine_factory,
)


class BizibleSnowFlakeExtractor:
    def __init__(self, config_dict: Dict):
        """

        :param config_dict: To be passed from the execute.py, should be ENV.
        :type config_dict:
        """
        self.bizible_engine = bizible_snowflake_engine_factory(
            config_dict, "BIZIBLE_USER"
        )
        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    def write_query_to_csv(self, engine, file_name, query):
        """
            Small function using the built-in CSV class rather than pandas.
        :param engine:
        :param file_name:
        :param query:
        :return:
        """
        connection = engine.connect()
        results = connection.execute(query)
        columns = results.keys()
        fetched_rows = results.fetchall()
        f = open(file_name, "w")
        out = csv.writer(f, delimiter="|")
        out.writerow([r for r in columns])

        for r in fetched_rows:
            out.writerow([c for c in r])

        connection.close()

    def upload_query(self, table_name: str, file_name: str, query: str) -> None:
        """

        :param file_name:
        :type file_name:
        :param table_name:
        :type table_name:
        :param query:
        :type query:
        """
        logging.info(f"Running {query}")
        self.write_query_to_csv(self.bizible_engine, file_name, query)

        logging.info(f"Processing {file_name} to {table_name}")
        snowflake_stage_load_copy_remove(
            file_name,
            "BIZIBLE.BIZIBLE_LOAD",
            f"BIZIBLE.{table_name.lower()}",
            self.snowflake_engine,
            "csv",
            file_format_options="trim_space=true field_optionally_enclosed_by = '0x22' SKIP_HEADER = 1 field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD = None",
        )

        logging.info(f"To delete {file_name}")
        os.remove(file_name)

    def upload_partitioned_files(
        self,
        table_name: str,
        start_date: datetime,
        end_date: datetime,
        date_column: str,
    ) -> None:
        """
        Created due to memory limitations, increments over the data set in hourly batches, primarily to ensure
        the BIZ.FACTS data size doesn't exceed what is available in K8
        :param table_name:
        :param start_date:
        :param end_date:
        :param date_column:
        :return:
        """
        time_increments = 30
        for dt in rrule.rrule(
            rrule.MINUTELY,
            dtstart=start_date,
            until=end_date,
            interval=time_increments,
        ):
            query_start_date = dt
            query_end_date = dt + timedelta(minutes=time_increments)

            query = f"""SELECT *, SYSDATE() as uploaded_at FROM BIZIBLE_ROI_V3.GITLAB.{table_name}
                WHERE {date_column} >= '{query_start_date}' 
                AND {date_column} < '{query_end_date}'"""

            file_name = f"{table_name}_{str(dt.year)}-{str(dt.month)}-{str(dt.day)}-{str(dt.hour)}-{str(dt.minute)}.csv"

            self.upload_query(table_name, file_name, query)

    def upload_complete_file(
        self,
        table_name: str,
    ) -> None:
        """

        :param table_name:
        :type table_name:
        """
        query = f"""
        SELECT *, SYSDATE() as uploaded_at FROM BIZIBLE_ROI_V3.GITLAB.{table_name}"""

        file_name = f"{table_name}.csv"
        self.upload_query(table_name, file_name, query)

    def process_bizible_file(
        self,
        start_date: datetime,
        end_date: datetime,
        table_name: Dict,
        date_column: str,
        full_refresh: bool = False,
    ) -> None:
        """

        :param start_date:
        :param end_date:
        :param table_name:
        :param date_column:
        :return:
        """
        logging.info(f"Running {table_name} query")

        if full_refresh:
            start_date = datetime.datetime(2020, 1, 1)

        if date_column == "":
            self.upload_complete_file(table_name)
        else:
            self.upload_partitioned_files(
                table_name,
                start_date,
                end_date,
                date_column,
            )
