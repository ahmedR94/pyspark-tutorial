import pytest
from src.main import Transformation

transformer = Transformation

@pytest.fixture(scope='session')
def spark():
    return transformer.spark
