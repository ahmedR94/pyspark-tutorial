"""PySpark unit tests"""
"""
    Create the input dataframe
    Create the output dataframe using the function we want to test
    Specify the expected output values
    Compare the results
"""

import pytest
from src.main import Transformation
from chispa.dataframe_comparer import assert_df_equality

input_path= "dataset/Chain_replacement.csv"
output_path = "dataset/output"
transformer = Transformation(input_path)


@pytest.mark.usefixtures('spark')
def test_read_file(spark):
    actual_df = spark.read.csv(input_path, header=True, sep="\t")
    actual_df = actual_df.drop("_c6")
    expected_df = transformer.read_file()
    assert_df_equality(actual_df, expected_df)


@pytest.mark.usefixtures('spark')
def test_apply_filters(spark_mock_df):
    input_df, excepted_df = spark_mock_df
    actual_df = transformer.apply_filters(dataset=input_df)
    assert_df_equality(actual_df, excepted_df)
