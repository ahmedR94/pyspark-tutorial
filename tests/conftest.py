""" Script where we can place our fixtures to structure and arrange our test case data. 
The fixture can be reusable within a test file. It’s convenient to reuse the same fixture for different test cases. 
The fixture can be both data like our above mock data function, 
or it could be a spark environment we would like to initiate for the test session. 
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.appName("Exercice Pyspark").getOrCreate()
    yield spark
    spark.stop()
