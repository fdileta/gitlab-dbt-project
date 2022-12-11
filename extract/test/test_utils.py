"""
The main test routine for utils for Automated Service Ping
"""

import pytest

from extract.saas_usage_ping.utils import EngineFactory


@pytest.fixture(autouse=True)
def create_engine_factory():
    return EngineFactory()


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
    assert create_engine_factory.connected == False


def test_connect(create_engine_factory):
    """
    Raise an error for connect as no connection data
    """

    with pytest.raises(KeyError):
        create_engine_factory.connect()
