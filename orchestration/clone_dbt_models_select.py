#!/usr/bin/env python3
import logging
import sys
import json
from os import environ as env
from typing import Dict, List

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from gitlabdata.orchestration_utils import query_executor
import argparse

from simple_dependency_resolver.simple_dependency_resolver import DependencyResolver


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class DbtModelClone:
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

    def create_schema(self, schema_name: str):
        """

        :param schema_name:
        :return:
        """
        logging.info("Creating schema if it does not exist")

        query = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
        query_executor(self.engine, query)

        logging.info("Granting rights on stage to TRANSFORMER")
        grants_query = f"""GRANT ALL ON SCHEMA {schema_name} TO TRANSFORMER;"""
        query_executor(self.engine, grants_query)

        logging.info("Granting rights on stage to GITLAB_CI")
        grants_query = f"""GRANT ALL ON SCHEMA {schema_name} TO GITLAB_CI"""
        query_executor(self.engine, grants_query)

        return True

    def clean_dbt_input(self, model_input: List[str]) -> List[Dict]:
        """

        :param model_input:
        :return:
        """
        joined = " ".join(model_input)

        delimeter = '{"depends_on":'

        input_list = [delimeter + x for x in joined.split(delimeter) if x]

        list_of_dicts = []
        for i in input_list:
            d = json.loads(i)
            actual_dependencies = [
                n for n in d.get("depends_on").get("nodes") if "seed" not in n
            ]
            d["actual_dependencies"] = actual_dependencies
            list_of_dicts.append(d)

        sorted_output = []

        for i in list_of_dicts:
            sorted_output.append(
                {"id": i.get("unique_id"), "dependencies": i.get("actual_dependencies")}
            )

        dr = DependencyResolver()
        resolved_dependencies = dr.simple_resolution(sorted_output)
        sorted_list = []

        for r in resolved_dependencies:
            for i in list_of_dicts:
                if i.get("unique_id") == r.name:
                    sorted_list.append(i)

        return sorted_list

    def grant_table_view_rights(self, object_type: str, object_name: str) -> None:
        """
            Right type can be table or view
        :param object_type:
        :param object_name:
        :return:
        """

        logging.info(f"Granting rights on {object_type} to TRANSFORMER")
        grants_query = f"""
            GRANT OWNERSHIP ON {object_type.upper()} {object_name.upper()} TO TRANSFORMER REVOKE CURRENT GRANTS
            """
        query_executor(self.engine, grants_query)

        logging.info(f"Granting rights on {object_type} to GITLAB_CI")
        grants_query = (
            f"""GRANT ALL ON {object_type.upper()} {object_name.upper()} TO GITLAB_CI"""
        )
        query_executor(self.engine, grants_query)

    def clean_view_dll(self, dll_input: str) -> str:
        """
        Essentially, this code is finding and replacing the DB name in only the first line for recreating
        views. This is because we have a database & schema named PREP, which creates a special case in the
        rest of the views they are replaced completely.

        :param dll_input:
        :return:
        """
        split_file = dll_input.splitlines()

        first_line = split_file[0]
        find_db_name = (
            first_line[dll_input.find("view") :]
            .split(".")[0]
            .replace("PREP", self.prep_database)
            .replace("PROD", self.prod_database)
        )
        new_first_line = f"{first_line[:dll_input.find('view')]}{find_db_name}{first_line[dll_input.find('.'):]}"

        replaced_file = [
            f.replace("PREP", self.prep_database).replace("PROD", self.prod_database)
            for f in split_file
        ]
        joined_lines = "\n".join(replaced_file[1:])

        output_query = new_first_line + "\n" + joined_lines

        return output_query

    def clone_dbt_models(self, model_input: List[str]):
        """

        :param model_input:
        :return:
        """

        sorted_list = self.clean_dbt_input(model_input)

        for i in sorted_list:
            database_name = i.get("database").upper()
            schema_name = i.get("schema").upper()
            table_name = i.get("name").upper()

            full_name = f"{database_name}.{schema_name}.{table_name}"

            output_table_name = f"{self.branch_name}_{full_name}"
            output_schema_name = output_table_name.replace(f".{table_name}", "")

            query = f"""
                SELECT
                    TABLE_TYPE,
                    IS_TRANSIENT
                FROM {database_name}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = UPPER('{schema_name}')
                AND TABLE_NAME = UPPER('{table_name}')
            """

            res = query_executor(self.engine, query)

            self.create_schema(output_schema_name)

            table_or_view = res[0][0]
            if table_or_view == "VIEW":
                logging.info("Cloning view")

                query = f"""SELECT GET_DDL('VIEW', '{full_name}', TRUE)"""
                res = query_executor(self.engine, query)

                base_dll = res[0][0]

                output_query = self.clean_view_dll(base_dll)
                query_executor(self.engine, output_query)
                logging.info(f"View {full_name} successfully created. ")

                self.grant_table_view_rights("view", output_table_name)

                continue

            transient_table = res[0][1]

            clone_statement = f"CREATE OR REPLACE {'TRANSIENT' if transient_table == 'YES' else ''} TABLE {output_table_name} CLONE {full_name} COPY GRANTS;"
            query_executor(self.engine, clone_statement)
            logging.info(f"{clone_statement} successfully run. ")

            self.grant_table_view_rights("table", output_table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT", nargs="+")
    args = parser.parse_args()

    cloner = DbtModelClone(env.copy())
    cloner.clone_dbt_models(args.INPUT)
