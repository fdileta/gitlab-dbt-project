import time
from logging import error

import numpy as np
import pandas as pd
from sqlalchemy.engine.base import Engine

from gitlabdata.orchestration_utils import (
    query_executor,
)


def table_has_changed(data: pd.DataFrame, engine: Engine, table: str) -> bool:
    """
    Check if the table has changed before uploading.
    """

    if engine.has_table(table):
        existing_table = pd.read_sql_table(table, engine)
        if "_updated_at" in existing_table.columns and existing_table.drop(
            "_updated_at", axis=1
        ).equals(data):
            info(f'Table "{table}" has not changed. Aborting upload.')
            return False
    return True


def dw_uploader(
    engine: Engine,
    table: str,
    data: pd.DataFrame,
    schema: str = "sheetload",
    chunk: int = 0,
    truncate: bool = False,
) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names and add metadata, generate the dtypes
    data.columns = [
        str(column_name).replace(" ", "_").replace("/", "_")
        for column_name in data.columns
    ]
    data = data.infer_objects()

    # Replace empty strings into NaN values which translates into NULL in SQL
    data.replace("", np.nan, inplace=True)

    # If the data isn't chunked, or this is the first iteration, drop table
    if not chunk and not truncate:
        table_changed = table_has_changed(data, engine, table)
        if not table_changed:
            return False
        drop_query = f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"
        query_executor(engine, drop_query)

    # Add the _updated_at metadata and set some vars if chunked
    data["_updated_at"] = time.time()
    if_exists = "append" if chunk else "replace"
    data.to_sql(
        name=table, con=engine, index=False, if_exists=if_exists, chunksize=15000
    )
    info(f"Successfully loaded {data.shape[0]} rows into {table}")
    return True


def translate_column_names(input: str):
    """
        Converts column names into a SnowFlake - parsable format.
    :param input:
    :return:
    """
    return input.strip().translate(input.maketrans(" /", "__"))  # can easily add more


def dw_uploader_append_only(
    engine: Engine,
    table: str,
    data: pd.DataFrame,
    chunk: int = 0,
) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names and add metadata, generate the dtypes
    data.columns = [
        translate_column_names(str(column_name))
        for column_name in data.columns
    ]
    data = data.infer_objects()

    # Replace empty strings into NaN values which translates into NULL in SQL
    data.replace("", np.nan, inplace=True)

    # Add the _updated_at metadata and set some vars if chunked
    data["_updated_at"] = time.time()
    if_exists = "append" if chunk else "replace"
    data.to_sql(
        name=table, con=engine, index=False, if_exists=if_exists, chunksize=15000
    )
    info(f"Successfully loaded {data.shape[0]} rows into {table}")
    return True
