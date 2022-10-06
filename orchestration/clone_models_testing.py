#!/usr/bin/env python3
import logging
import sys
import json
from os import environ as env
from typing import Dict, List

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

from gitlabdata.orchestration_utils import query_executor
import argparse


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)

parser = argparse.ArgumentParser()
parser.add_argument('first_string', nargs='+')
args = parser.parse_args()

print(args.first_string)


class SnowflakeManager:
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
            URL(
                user=config_vars["SNOWFLAKE_USER"],
                password=config_vars["SNOWFLAKE_PASSWORD"],
                account=config_vars["SNOWFLAKE_ACCOUNT"],
                role=config_vars["SNOWFLAKE_SYSADMIN_ROLE"],
                warehouse=config_vars["SNOWFLAKE_LOAD_WAREHOUSE"],
            )
        )

        # Snowflake database name should be in CAPS
        # see https://gitlab.com/meltano/analytics/issues/491
        self.branch_name = config_vars["BRANCH_NAME"].upper()
        self.prep_database = "{}_PREP".format(self.branch_name)
        self.prod_database = "{}_PROD".format(self.branch_name)
        self.raw_database = "{}_RAW".format(self.branch_name)



    def clone_models_v2_testing(self, *model_input):


        input_list = list(model_input)
        print(input_list)

        convert = ''

        for i in input_list:
            convert = convert + i

        print(convert)



if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
