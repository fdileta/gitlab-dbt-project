import logging
import sys
from os import environ as env

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_copy_command(model, sensitive, timestamp, inc_start, inc_end, stage, single):
    """
    Generate a copy command based on data passed from pumps.yml
    """
    try:
        logging.info("Getting copy command...")

        from_statement = "FROM PROD.{schema}.{model}".format(
            model=model, schema="pumps" if sensitive == False else "pumps_sensitive"
        )

        where_statement = (
            " WHERE {timestamp} between '{inc_start}' and '{inc_end}'".format(
                timestamp=timestamp,
                inc_start=inc_start,
                inc_end=inc_end,
            )
        )

        if timestamp == None:
            query = "SELECT * " + from_statement
        else:
            query = "SELECT * " + from_statement + where_statement

        if single == False:
            target_name = model
            option = "INCLUDE_QUERY_ID"
        else:
            target_name = f"{timestamp}.csv"
            option = "SINGLE"

        copy_command_tmp = """
        COPY INTO @RAW.PUBLIC.{stage}/{target_name}
        FROM ({query} LIMIT 1000000)
        FILE_FORMAT = (TYPE = CSV, NULL_IF = (), FIELD_OPTIONALLY_ENCLOSED_BY = '"', COMPRESSION=NONE)
        HEADER = TRUE
        {option} = TRUE
        ;
      """

        copy_command = copy_command_tmp.format(
            stage=stage.upper(),
            model=model,
            query=query,
            target_name=target_name,
            option=option
        )

    except:
        logging.info("Failed to get copy command...")
    finally:
        return copy_command


def copy_data(model, sensitive, timestamp, inc_start, inc_end, stage):
    """
    run copy command to copy data from snowflake
    """
    logging.info("Preparing copy data...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    try:
        connection = engine.connect()
        copy_command = get_copy_command(
            model, sensitive, timestamp, inc_start, inc_end, stage
        )
        logging.info(f"running copy command {copy_command}")
        connection.execute(copy_command).fetchone()
    except:
        logging.info("Failed to run copy command...")
        raise
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(copy_data)
    logging.info("Complete.")
