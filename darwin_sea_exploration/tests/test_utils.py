from darwin_sea_exploration.main import *
import pytest

def test_basic_utils():
    expected_value = True
    actual_value = True
    assert actual_value == expected_value, f'Expected Value {expected_value}, Actual Value {actual_value}'