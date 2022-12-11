"""
The main test routine for utils for Automated Service Ping
"""

import os
from unittest import mock

import pytest
import requests
import responses

from extract.saas_usage_ping.utils import EngineFactory, Utils


@pytest.fixture(autouse=True)
def create_engine_factory():
    """
    Create class object
    """
    return EngineFactory()


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    """
    Simulate OS env. variables
    """
    with mock.patch.dict(
        os.environ,
        {"GITLAB_ANALYTICS_PRIVATE_TOKEN": "xxx"},
    ):
        yield


@pytest.fixture(autouse=True)
def create_utils(mock_settings_env_vars):
    """
    Create class object
    """
    _ = mock_settings_env_vars

    return Utils()


@pytest.fixture
def mocked_responses():
    """
    Mock routine to create fake response
    """
    with responses.RequestsMock() as rsps:
        yield rsps


def test_engine_factory(create_engine_factory):
    """
    Test Class creation
    """
    assert create_engine_factory is not None


def test_engine_factory_processing_warehouse(create_engine_factory):
    """
    Test Class properties - processing_warehouse
    """
    assert create_engine_factory.processing_warehouse == "LOADER"


def test_engine_factory_schema_name(create_engine_factory):
    """
    Test Class properties - schema_name
    """
    assert create_engine_factory.schema_name == "saas_usage_ping"


def test_engine_factory_loader_engine(create_engine_factory):
    """
    Test Class properties - loader_engine
    """
    assert create_engine_factory.loader_engine is None


def test_engine_factory_config_vars(create_engine_factory):
    """
    Test Class properties - config_vars
    """
    assert create_engine_factory.config_vars is not None


def test_engine_factory_connected(create_engine_factory):
    """
    Test Class properties - connected
    """
    assert create_engine_factory.connected is False


def test_connect(create_engine_factory):
    """
    Raise an error for connect as no connection data
    """

    with pytest.raises(KeyError):
        create_engine_factory.connect()


def test_utils(create_utils):
    """
    Test Class creation
    """
    assert create_utils is not None


def test_headers(create_utils):
    """
    Test Class properties - headers
    """
    assert create_utils.headers["PRIVATE-TOKEN"] == "xxx"


def test_headers_error(create_utils):
    """
    Test Class properties - headers
    """
    with pytest.raises(KeyError):
        assert create_utils.headers["WRONG_KEY"] == "xxx"


def test_convert_response_to_json(create_utils, mocked_responses):

    """
    Test function: convert_response_to_json
    """
    expected = {"didi": "pro", "test": "1"}
    mocked_responses.get(
        "http://some_gitlab_api_url/test",
        body='{"test": "1", "didi": "pro"}',
        status=200,
        content_type="application/json",
    )

    resp = requests.get("http://some_gitlab_api_url/test")

    actual = create_utils.convert_response_to_json(response=resp)

    assert actual == expected


def test_get_response(create_utils):
    """
    Force fake url and raise a Connection Error
    """
    with pytest.raises(ConnectionError):
        _ = create_utils.get_response("http://fake_url/test")
