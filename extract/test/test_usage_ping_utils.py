"""
The main test routine for utils for Automated Service Ping
"""

import pytest

from extract.saas_usage_ping.usage_ping_utils import EngineFactory

@pytest.fixture(autouse=True)
def create_engine_factory():
    return EngineFactory()


def test_engine_factory(create_engine_factory):
    """
    Test Class creation
    """
    assert create_engine_factory is not None

def test_engine_factory_properties(create_engine_factory):
    """
    Test Class properties
    """
    create_engine_factory.env_copy()

    assert create_engine_factory.__dict__.get("PROCESSING_WAREHOUSE") == "LOADER"
    assert create_engine_factory.__dict__.get("config_vars") is not None