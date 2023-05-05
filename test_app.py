"""PySpark unit tests"""
"""
    Create the input dataframe
    Create the output dataframe using the function we want to test
    Specify the expected output values
    Compare the results
"""

import pandas as pd
from main import Transformation

input_path= "dataset/Chain_replacement.csv"
output_path = "dataset/output"
transformer = Transformation(input_path)


def test_read_file():
    df = transformer.read_file()
    assert df.columns == [
        'REPLACING-RENAULT-REF',
        'REPLACING-SUPPLIER-REF',
        'REPLACING-SUPPLIER-NAME',
        'REPLACED-RENAULT-REF',
        'REPLACED-SUPPLIER-REF',
        'REPLACEMENT-DATE'
        ]

def test_apply_filters():
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
    input_df = transformer.spark.createDataFrame(input_data).toDF(*input_columns)
    output_columns = input_columns + ["SHIPPING_DATE"]
    data = [("8660000025","EMPTY","SHELL","8671013785","NOUVEAU","03/11/2014","2023"), 
            ("8660000710","437444","Valeo","8671000000","EMPTY","13/02/2018","2024"), 
            ("8660000713","EMPTY","CLOUD","8671000003","CONSOMMABLES","13/02/2014","2022")]
    excepted_df = transformer.spark.createDataFrame(data).toDF(*output_columns)
    expected_pandas = excepted_df.toPandas()
    output_df = transformer.apply_filters(dataset=input_df)
    output_pandas = output_df.toPandas()
    pd.testing.assert_frame_equal(output_pandas, expected_pandas, check_exact=True)
