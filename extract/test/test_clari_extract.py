"""
Test clari extract
"""
from datetime import datetime

import pytest
import requests

from extract.clari.src.clari import (
    _calc_fiscal_quarter,
    _get_previous_fiscal_quarter,
    get_fiscal_quarter,
    config_dict,
    make_request,
    check_valid_quarter,
)


def test_calc_fiscal_quarter1():
    """Check fiscal quarter1 logic"""
    # test Q1
    actual_q1 = "2021_Q1"

    datetime_q1_1 = datetime(2020, 2, 1)
    res_q1_1 = _calc_fiscal_quarter(datetime_q1_1)
    assert res_q1_1 == actual_q1

    datetime_q1_2 = datetime(2020, 3, 1)
    res_q1_2 = _calc_fiscal_quarter(datetime_q1_2)
    assert res_q1_2 == actual_q1

    datetime_q1_3 = datetime(2020, 4, 1)
    res_q1_3 = _calc_fiscal_quarter(datetime_q1_3)
    assert res_q1_3 == actual_q1


def test_calc_fiscal_quarter2():
    """Check fiscal quarter2 logic"""
    # test Q2
    actual_q2 = "2021_Q2"

    datetime_q2_1 = datetime(2020, 5, 1)
    res_q2_1 = _calc_fiscal_quarter(datetime_q2_1)
    assert res_q2_1 == actual_q2

    datetime_q2_2 = datetime(2020, 6, 1)
    res_q2_2 = _calc_fiscal_quarter(datetime_q2_2)
    assert res_q2_2 == actual_q2

    datetime_q2_3 = datetime(2020, 7, 1)
    res_q2_3 = _calc_fiscal_quarter(datetime_q2_3)
    assert res_q2_3 == actual_q2


def test_calc_fiscal_quarter3():
    """Check fiscal quarter3 logic"""
    # test Q3
    actual_q3 = "2021_Q3"

    datetime_q3_1 = datetime(2020, 8, 1)
    res_q3_1 = _calc_fiscal_quarter(datetime_q3_1)
    assert res_q3_1 == actual_q3

    datetime_q3_2 = datetime(2020, 9, 1)
    res_q3_2 = _calc_fiscal_quarter(datetime_q3_2)
    assert res_q3_2 == actual_q3

    datetime_q3_3 = datetime(2020, 10, 1)
    res_q3_3 = _calc_fiscal_quarter(datetime_q3_3)
    assert res_q3_3 == actual_q3


def test_calc_fiscal_quarter4():
    """Check fiscal quarter4 logic"""
    # test Q4
    actual_q4 = "2021_Q4"

    datetime_q4_1 = datetime(2020, 11, 1)
    res_q4_1 = _calc_fiscal_quarter(datetime_q4_1)
    assert res_q4_1 == actual_q4

    datetime_q4_2 = datetime(2020, 12, 1)
    res_q4_2 = _calc_fiscal_quarter(datetime_q4_2)
    assert res_q4_2 == actual_q4

    datetime_q4_3 = datetime(2021, 1, 1)
    res_q4_3 = _calc_fiscal_quarter(datetime_q4_3)
    assert res_q4_3 == actual_q4


def test_get_previous_fiscal_quarter():
    """Check previous fiscal quarter logic"""
    datetime_q1 = datetime(2020, 2, 1)
    res_q1 = _get_previous_fiscal_quarter(datetime_q1)
    actual_q1 = "2020_Q4"
    assert res_q1 == actual_q1

    datetime_q2 = datetime(2020, 5, 1)
    res_q2 = _get_previous_fiscal_quarter(datetime_q2)
    actual_q2 = "2021_Q1"
    assert res_q2 == actual_q2

    datetime_q3 = datetime(2020, 8, 1)
    res_q3 = _get_previous_fiscal_quarter(datetime_q3)
    actual_q3 = "2021_Q2"
    assert res_q3 == actual_q3

    datetime_q4 = datetime(2020, 11, 1)
    res_q4 = _get_previous_fiscal_quarter(datetime_q4)
    actual_q4 = "2021_Q3"
    assert res_q4 == actual_q4


def test_get_fiscal_quarter():
    """Get fiscal quarter based on env var"""
    config_dict["task_schedule"] = "daily"

    config_dict["execution_date"] = datetime(2020, 2, 1).strftime("%Y-%m-%d %H:%M:%S")
    res_q1 = get_fiscal_quarter()
    actual_q1 = "2021_Q1"
    assert res_q1 == actual_q1

    config_dict["execution_date"] = datetime(2020, 5, 1).strftime("%Y-%m-%d %H:%M:%S")
    res_q2 = get_fiscal_quarter()
    actual_q2 = "2021_Q2"
    assert res_q2 == actual_q2

    config_dict["execution_date"] = datetime(2020, 8, 1).strftime("%Y-%m-%d %H:%M:%S")
    res_q3 = get_fiscal_quarter()
    actual_q3 = "2021_Q3"
    assert res_q3 == actual_q3

    config_dict["execution_date"] = datetime(2020, 11, 1).strftime("%Y-%m-%d %H:%M:%S")
    res_q4 = get_fiscal_quarter()
    actual_q4 = "2021_Q4"
    assert res_q4 == actual_q4


def test_make_request():
    """Test requests"""
    # Test that google request passes
    request_type = "GET"
    url = "https://www.google.com"
    resp = make_request(request_type, url)
    assert resp.status_code == 200

    # Test that an invalid request type throws an error
    error_str = "Invalid request type"
    with pytest.raises(ValueError) as exc:
        request_type = "nonexistent_request_type"
        make_request(request_type, url)
    assert error_str in str(exc.value)

    # Test HTTP error
    request_type = "GET"
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        url = "https://www.google.com/invalid_url"
        make_request(request_type, url)
    error_str = "404 Client Error"
    assert error_str in str(exc.value)


def test_check_valid_quarter():
    """Test valid quarter check"""
    results_dict = {
        "timePeriods": [
            {
                "timePeriodId": "2023_Q4",
                "type": "quarter",
                "label": "Quarter 4",
                "year": "2023",
                "startDate": "2022-11-01",
                "endDate": "2023-01-31",
                "crmId": "0264M000001KFm1QAG",
            }
        ]
    }
    original_fiscal_quarter = "2023_Q4"
    assert check_valid_quarter(original_fiscal_quarter, results_dict) is None
    with pytest.raises(ValueError) as exc:
        original_fiscal_quarter = "2022_Q4"
        error_str = "does not match"
        check_valid_quarter(original_fiscal_quarter, results_dict)
    assert error_str in str(exc.value)
