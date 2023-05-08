""" Script where we can place our fixtures to structure and arrange our test case data. 
The fixture can be reusable within a test file. It’s convenient to reuse the same fixture for different test cases. 
The fixture can be both data like our above mock data function, 
or it could be a spark environment we would like to initiate for the test session. 
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("Exercice Pyspark").getOrCreate()


@pytest.fixture
def spark_mock_df(spark):

    input_columns = [
        "REPLACING-RENAULT-REF", "REPLACING-SUPPLIER-REF", "REPLACING-SUPPLIER-NAME",
        "REPLACED-RENAULT-REF",	"REPLACED-SUPPLIER-REF", "REPLACEMENT-DATE", 	
    ]
    input_data = [
        (None, '#N/A', 'SHELL', '8671013783', 'NOUVEAU', '03/11/2014'),
        (None, '#N/A', 'SHELL', 'xxx', 'NOUVEAU', '03/11/2014'),
        ('8660000025', '#N/A', 'SHELL', '8671013785', 'NOUVEAU','03/11/2014'),
        ('8660000710', '437444', 'Valeo', '8671000020', '#N/A','13/02/2019'),
        ('8660000710', '437444', 'Valeo', '8671000000', '#N/A','13/02/2018'),
        ('0', '#N/A', 'INCONNU AM', '8671000000', 'CONSOMMABLES','13/02/2014'),
        ('0', '#N/A', 'AWS', '8671000001', 'CONSOMMABLES', '13/02/2014'),
        ('8660000712', '#N/A', 'aws', '8671000002', 'CONSOMMABLES','13/02/2014'),
        ('8660000713', '#N/A', 'CLOUD', '8671000003', 'CONSOMMABLES','13/02/2014')
    ]
    df = spark.createDataFrame(input_data).toDF(*input_columns)
    output_columns = input_columns + ["SHIPPING_DATE"]
    output_data = [("8660000025","EMPTY","SHELL","8671013785","NOUVEAU","03/11/2014","2023"), 
            ("8660000710","437444","Valeo","8671000000","EMPTY","13/02/2018","2024"), 
            ("8660000713","EMPTY","CLOUD","8671000003","CONSOMMABLES","13/02/2014","2022")]
    excepted_df = spark.createDataFrame(output_data).toDF(*output_columns)
    return df, excepted_df
