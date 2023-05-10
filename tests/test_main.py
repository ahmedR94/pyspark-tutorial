""" PySpark unit tests
    Create the input dataframe
    Create the output dataframe using the function we want to test
    Specify the expected output values
    Compare the results
"""

import os
import pytest
from src.main import Transformation, set_env_vars
from chispa.dataframe_comparer import assert_df_equality


set_env_vars()
input_path= os.getenv("input_path_dataset")
transformer = Transformation(input_path)


@pytest.mark.usefixtures('spark')
def test_read_file(spark):
    data = [{'name': 'Alice', 'id': "1"},{'name': 'Mario', 'id': "2"}]
    expected_df = spark.createDataFrame(data)
    transf = Transformation('dataset/test_dataset.csv')
    actual_df = transf.read_file()
    assert_df_equality(actual_df, expected_df, ignore_row_order=True,ignore_column_order=True)


@pytest.mark.usefixtures('spark')
def test_apply_filters(spark):
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
    input_df = spark.createDataFrame(input_data).toDF(*input_columns)
    output_columns = input_columns + ["SHIPPING_DATE"]
    output_data = [("8660000025","EMPTY","SHELL","8671013785","NOUVEAU","03/11/2014","2023"), 
            ("8660000710","437444","Valeo","8671000000","EMPTY","13/02/2018","2024"), 
            ("8660000713","EMPTY","CLOUD","8671000003","CONSOMMABLES","13/02/2014","2022")]
    excepted_df = spark.createDataFrame(output_data).toDF(*output_columns)
    actual_df = transformer.apply_filters(dataset=input_df)
    assert_df_equality(actual_df, excepted_df)


@pytest.mark.usefixtures('spark')
def test_save_dataframe_to_csv(spark):
    destination_path = 'dataset/output/test'
    data = [{'name': 'Alice', 'id': "1"},{'name': 'Mario', 'id': "2"}]
    expected_df = spark.createDataFrame(data)
    transformer.save_dataframe_to_csv(expected_df,destination_path)
    actual_df = spark.read.csv(destination_path,header=True)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
