import pytest
import sys
import os
from datetime import datetime

# Tweak path as due to script execution way in Airflow, can't touch the original code
abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + "/extract/saas_usage_ping"
sys.path.append(abs_path)

from extract.saas_usage_ping.usage_ping import UsagePing


def test_get_md5():
    usage_ping_test = UsagePing

    input_timestamps = [
        datetime(2021, 9, 1, 23, 10, 21).timestamp(),
        datetime(2020, 8, 1, 23, 10, 22).timestamp(),
        datetime(2021, 7, 1, 23, 10, 23).timestamp(),
        "test_string",
        "",
        None,
    ]

    """
    Know testing the private method is not aligned with best praxis, but found it is sufficient
    in this implementation.
    """
    for i, check_time in enumerate(input_timestamps):
        res = usage_ping_test._get_md5(None, check_time)
        # Check output data type
        assert isinstance(res, str)
        # Check is len 32 as it is expected length
        assert len(res) == 32  # bytes in hex representation
        # As this is one-way function, can't test it with many things - let see to we have all details with various inputs
        assert res is not None
