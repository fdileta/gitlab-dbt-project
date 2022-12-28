import os
import pytest
import requests

from datetime import datetime, timedelta
from unittest.mock import Mock

from extract.clari.src.clari import (
    _calc_fiscal_quarter, _get_previous_fiscal_quarter, get_fiscal_quarter,
    config_dict, make_request, check_valid_quarter
)


def test_calc_fiscal_quarter():
    """
    Check static variables
    """
    datetime_q1 = datetime(2020, 2, 1)
    res_q1 = _calc_fiscal_quarter(datetime_q1)
    actual_q1 = '2021_Q1'
    assert res_q1 == actual_q1

    datetime_q2 = datetime(2020, 5, 1)
    res_q2 = _calc_fiscal_quarter(datetime_q2)
    actual_q2 = '2021_Q2'
    assert res_q2 == actual_q2

    datetime_q3 = datetime(2020, 8, 1)
    res_q3 = _calc_fiscal_quarter(datetime_q3)
    actual_q3 = '2021_Q3'
    assert res_q3 == actual_q3

    datetime_q4 = datetime(2020, 11, 1)
    res_q4 = _calc_fiscal_quarter(datetime_q4)
    actual_q4 = '2021_Q4'
    assert res_q4 == actual_q4


def test_get_previous_fiscal_quarter():
    """
    Check static variables
    """
    datetime_q1 = datetime(2020, 2, 1)
    res_q1 = _get_previous_fiscal_quarter(datetime_q1)
    actual_q1 = '2020_Q4'
    assert res_q1 == actual_q1

    datetime_q2 = datetime(2020, 5, 1)
    res_q2 = _get_previous_fiscal_quarter(datetime_q2)
    actual_q2 = '2021_Q1'
    assert res_q2 == actual_q2

    datetime_q3 = datetime(2020, 8, 1)
    res_q3 = _get_previous_fiscal_quarter(datetime_q3)
    actual_q3 = '2021_Q2'
    assert res_q3 == actual_q3

    datetime_q4 = datetime(2020, 11, 1)
    res_q4 = _get_previous_fiscal_quarter(datetime_q4)
    actual_q4 = '2021_Q3'
    assert res_q4 == actual_q4


def test_get_fiscal_quarter():
    """
    Check static variables
    """
    config_dict['task_schedule'] = 'daily'

    config_dict['execution_date'] = datetime(2020, 2,
                                             1).strftime("%Y-%m-%d %H:%M:%S")
    res_q1 = get_fiscal_quarter()
    actual_q1 = '2021_Q1'
    assert res_q1 == actual_q1

    config_dict['execution_date'] = datetime(2020, 5,
                                             1).strftime("%Y-%m-%d %H:%M:%S")
    res_q2 = get_fiscal_quarter()
    actual_q2 = '2021_Q2'
    assert res_q2 == actual_q2

    config_dict['execution_date'] = datetime(2020, 8,
                                             1).strftime("%Y-%m-%d %H:%M:%S")
    res_q3 = get_fiscal_quarter()
    actual_q3 = '2021_Q3'
    assert res_q3 == actual_q3

    config_dict['execution_date'] = datetime(2020, 11,
                                             1).strftime("%Y-%m-%d %H:%M:%S")
    res_q4 = get_fiscal_quarter()
    actual_q4 = '2021_Q4'
    assert res_q4 == actual_q4


def test_make_request():
    # Test that google request passes
    request_type = 'GET'
    url = "https://www.google.com"
    resp = make_request(request_type, url)
    assert resp.status_code == 200

    # Test that an invalid request type throws an error
    error_str = "Invalid request type"
    with pytest.raises(ValueError) as exc:
        request_type = 'nonexistent_request_type'
        make_request(request_type, url)
    assert error_str in str(exc.value)

    # Test HTTP error
    request_type = 'GET'
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        url = "https://www.google.com/invalid_url"
        make_request(request_type, url)
    error_str = '404 Client Error'
    assert error_str in str(exc.value)


def test_check_valid_quarter():
    results_dict = {
        "timePeriods":
            [
                {
                    "timePeriodId": "2023_Q4",
                    "type": "quarter",
                    "label": "Quarter 4",
                    "year": "2023",
                    "startDate": "2022-11-01",
                    "endDate": "2023-01-31",
                    "crmId": "0264M000001KFm1QAG"
                }
            ]
    }
    original_fiscal_quarter = '2023_Q4'
    assert check_valid_quarter(original_fiscal_quarter, results_dict) is None
