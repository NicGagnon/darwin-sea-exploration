# third party imports
import pytest


@pytest.fixture
def sample_data():
    """ Will return only the necessary fields that are actually accessed """
    data = []
    return data