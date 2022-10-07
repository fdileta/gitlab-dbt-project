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



    def clone_models_v2_testing(self, model_input):

        print(model_input)
        print(type(model_input))
        print(model_input[0])
        print(len(model_input[0]))

        joined = ' '.join(model_input)
        print(joined)

        input_list = model_input
        output_list = []
        for i in input_list:
            d = json.loads(i)
            actual_dependencies = [n for n in d.get('depends_on').get('nodes') if 'seed' not in n]
            d["actual_dependencies"] = actual_dependencies
            output_list.append(d)

        for s in sorted(output_list, key=lambda i: len(i['actual_dependencies'])):
            print(len(s.get('actual_dependencies')))



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('INPUT', nargs='+')
    args = parser.parse_args()

    snowflake_manager = SnowflakeManager(env.copy())
    snowflake_manager.clone_models_v2_testing(args.INPUT)
