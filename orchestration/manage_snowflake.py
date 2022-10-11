#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict, List

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

from gitlabdata.orchestration_utils import query_executor


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
        self.prep_database = "{}_PREP".format(config_vars["BRANCH_NAME"].upper())
        self.prod_database = "{}_PROD".format(config_vars["BRANCH_NAME"].upper())
        self.raw_database = "{}_RAW".format(config_vars["BRANCH_NAME"].upper())

    def generate_db_queries(
        self,
        database_name: str,
        cloned_database: str,
        optional_schema_to_clone: str,
        source_db: str,
    ) -> List[str]:
        """
        Generate the queries to clone and provide permissions for databases.
        """

        # Queries for database cloning and permissions
        check_db_exists_query = """use database "{0}";"""
        create_query = """create or replace database "{0}" {1};"""
        grant_query = """grant ownership on database "{0}" to TRANSFORMER;"""

        clone_schema_query = """create schema "{0}"."{1}" clone "{2}"."{1}"; """

        usage_roles = ["LOADER", "TRANSFORMER", "ENGINEER"]
        usage_grant_query_with_params = (
            """grant create schema, usage on database "{0}" to {1}"""
        )
        usage_grant_queries = [
            usage_grant_query_with_params.format(database_name, role)
            for role in usage_roles
        ]

        # The order of the queries matters!
        queries = [
            check_db_exists_query.format(database_name),
            create_query.format(database_name, cloned_database),
            grant_query.format(database_name),
        ] + usage_grant_queries

        if optional_schema_to_clone != "":
            create_schema = clone_schema_query.format(
                database_name.upper(),
                optional_schema_to_clone.upper(),
                source_db.upper(),
            )
            grant_on_schema_query_with_params = (
                """grant create table, usage on schema "{0}"."{1}" to "{2}";"""
            )
            schema_grants = [
                grant_on_schema_query_with_params.format(
                    database_name.upper(), optional_schema_to_clone.upper(), role
                )
                for role in usage_roles
            ]
            future_grant_on_tables_in_schema = """grant ALL PRIVILEGES on future tables in schema "{0}"."{1}" to role {2};"""
            schema_grants = schema_grants + [
                future_grant_on_tables_in_schema.format(
                    database_name.upper(), optional_schema_to_clone.upper(), role
                )
                for role in usage_roles
            ]
            queries = queries + [create_schema] + schema_grants

        return queries

    def manage_clones(
        self,
        database: str,
        empty: bool = False,
        force: bool = False,
        schema: str = "",
        include_stages: bool = False,
    ) -> None:
        """
        For the creation of zero copy clones in Snowflake.
        """

        if schema != "":
            empty = True

        databases = {
            "prep": self.prep_database,
            "prod": self.prod_database,
            "raw": self.raw_database,
        }

        create_db = databases[database]
        clone_db = f"clone {database}" if not empty else ""
        queries = self.generate_db_queries(create_db, clone_db, schema, database)

        # if force is false, check if the database exists
        if force:
            logging.info("Forcing a create or replace...")
            db_exists = False
        else:
            try:
                logging.info("Checking if DB exists...")
                connection = self.engine.connect()
                connection.execute(queries[0])
                logging.info("DBs exist...")
                db_exists = True
            except:
                logging.info("DB does not exist...")
                db_exists = False
            finally:
                connection.close()
                self.engine.dispose()

        # If the DB doesn't exist or --force is true, create or replace the db
        if not db_exists:
            logging.info("Creating or replacing DBs")
            for query in queries[1:]:
                try:
                    logging.info("Executing Query: {}".format(query))
                    connection = self.engine.connect()
                    [result] = connection.execute(query).fetchone()
                    logging.info("Query Result: {}".format(result))
                finally:
                    connection.close()
                    self.engine.dispose()

            if include_stages:
                self.clone_stages(create_db, database, schema)

    def delete_clones(self):
        """
        Delete a clone.
        """
        db_list = [
            self.prep_database,
            self.prod_database,
            self.raw_database,
        ]

        for db in db_list:
            query = 'DROP DATABASE IF EXISTS "{}";'.format(db)
            try:
                logging.info("Executing Query: {}".format(query))
                connection = self.engine.connect()
                [result] = connection.execute(query).fetchone()
                logging.info("Query Result: {}".format(result))
            finally:
                connection.close()
                self.engine.dispose()

    def create_table_clone(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_table: str,
        target_schema: str = None,
        timestamp: str = None,
    ):
        """
        Create a zero copy clone of a table (optionally at a given timestamp)
        source_database: database of table to be cloned
        source_schema: schema of table to be cloned
        source_table: name of table to cloned
        target_database: name of clone database
        target_table: name of clone table
        target_schema: schema of clone table
        timestamp: timestamp indicating time of clone in format yyyy-mm-dd hh:mi:ss
        """
        timestamp_format = """yyyy-mm-dd hh:mi:ss"""
        if not target_schema:
            target_schema = source_schema

        queries = []
        # Tries to create the schema its about to write to
        # If it does exists, {schema} already exists, statement succeeded.
        # is returned.
        schema_check = (
            f"""CREATE SCHEMA IF NOT EXISTS "{target_database}".{target_schema};"""
        )
        queries.append(schema_check)

        clone_sql = f"""create table if not exists {target_database}.{target_schema}.{target_table} clone "{source_database}".{source_schema}.{source_table}"""
        if timestamp and timestamp_format:
            clone_sql += f""" at (timestamp => to_timestamp_tz('{timestamp}', '{timestamp_format}'))"""
        clone_sql += " COPY GRANTS;"
        # Drop statement for safety
        queries.append(
            f"drop table if exists {target_database}.{target_schema}.{target_table};"
        )
        queries.append(clone_sql)
        connection = self.engine.connect()
        try:
            for q in queries:
                logging.info("Executing Query: {}".format(q))
                [result] = connection.execute(q).fetchone()
                logging.info("Query Result: {}".format(result))
        finally:
            connection.close()
            self.engine.dispose()

        return self

    def clone_stages(self, create_db: str, database: str, schema: str = ""):
        """
         Clones the stages available in a DB or schema (if specified).
         Required as SnowFlake currently does not support the cloning of internal stages.
        :param create_db:
        :param database:
        :param schema:
        """
        stages_query = f"""
             SELECT 
                 stage_schema,
                 stage_name,
                 stage_url, 
                 stage_type
             FROM {database}.information_schema.stages
             {f"WHERE stage_schema = '{schema.upper()}'" if schema != "" else ""}
             """

        stages = query_executor(self.engine, stages_query)

        for stage in stages:

            output_stage_name = (
                f""" "{create_db}"."{stage['stage_schema']}"."{stage['stage_name']}" """
            )
            from_stage_name = f""" "{database.upper()}"."{stage['stage_schema']}"."{stage['stage_name']}" """

            if stage["stage_type"] == "External Named":

                clone_stage_query = f"""
                    CREATE OR REPLACE STAGE {output_stage_name} LIKE   
                    {from_stage_name}
                    """

                grants_query = f"""
                    GRANT USAGE ON STAGE {output_stage_name} TO LOADER
                    """

            else:
                clone_stage_query = f"""
                    CREATE OR REPLACE STAGE {output_stage_name}  
                    """

                grants_query = f"""
                    GRANT READ, WRITE ON STAGE {output_stage_name} TO LOADER
                    """

            try:
                logging.info(f"Creating stage {output_stage_name}")
                res = query_executor(self.engine, clone_stage_query)
                logging.info(res[0])

                logging.info("Granting rights on stage to LOADER")
                res = query_executor(self.engine, grants_query)
                logging.info(res[0])

            except ProgrammingError as prg:
                # Catches permissions errors
                logging.error(prg._sql_message(as_unicode=False))


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
