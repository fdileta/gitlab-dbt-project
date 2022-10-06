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


