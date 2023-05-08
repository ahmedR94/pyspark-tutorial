"""main script"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, length, udf
from pyspark.sql.types import StringType
import json


def set_env_vars():
    for k, v in json.load(open('config/config.json')).items():
        os.environ[k] = v


class Transformation:
    spark = SparkSession.builder.appName("Exercice Pyspark").getOrCreate()

    def __init__(self, file_path:str):
        """constructor"""
        self.file_path = file_path

    def read_file(self) -> DataFrame :
        """Loads the csv file and returns the result as a DataFrame"""
        dataset = self.spark.read.csv(self.file_path, header=True, sep="\t")
        dataset = dataset.drop("_c6")
        return dataset

    def apply_filters(self,dataset=None) -> DataFrame :
        """apply all transformations on the dataset"""
        if dataset is None:
            dataset = self.read_file()
        # question 1
        dataset1 = dataset.filter(length(col("REPLACING-RENAULT-REF")) == 10)
        # question 2
        deleted_vals = ["aws", "AWS", "Aws"]
        dataset2 = dataset1.filter(~col("REPLACING-SUPPLIER-NAME").isin(deleted_vals))
        # question 3
        def shipping_date(replaced_supplier_ref):
            if replaced_supplier_ref == "CONSOMMABLES":
                ship_date = "2022"
            elif replaced_supplier_ref == "NOUVEAU":
                ship_date = "2023"
            else:
                ship_date = "2024"
            return ship_date

        shipping_dateUDF = udf(lambda x: shipping_date(x), StringType())
        dataset3 = dataset2.withColumn(
            "SHIPPING_DATE", shipping_dateUDF(col("REPLACED-SUPPLIER-REF"))
        )
        # question 4
        def new_date(date):
            """retun date of format 'd/m/Y' to 'Y-m-d 00:00:00'"""
            new_date = date.split("/")
            new_date = (
                new_date[2] + "-" + new_date[1] + "-" + new_date[0] + "-" + " 00:00:00"
            )
            return new_date

        new_dateUDF = udf(lambda x: new_date(x), StringType())
        dataset4 = (
            dataset3.withColumn("new_date", new_dateUDF(col("REPLACEMENT-DATE")))
            .sort(col("new_date").asc())
            .dropDuplicates(["REPLACING-RENAULT-REF"])
            .drop("new_date")
        )
        # question 5
        def fill_na(val):
            if val == "#N/A":
                new = "EMPTY"
            else:
                new = val
            return new

        fillUDF = udf(lambda x: fill_na(x), StringType())
        for column in dataset4.columns:
            dataset4 = dataset4.withColumn(column, fillUDF(col(column)))

        return dataset4

    @staticmethod
    def save_dataframe_to_csv(dataframe: DataFrame, destination_path: str)  -> None :
        """save pyspark dataframe in csv file"""
        dataframe.write.mode("overwrite").csv(destination_path, header=True)


if __name__ == "__main__":
    set_env_vars()
    input_path= os.getenv("input_path_dataset")
    output_path = os.getenv("output_path_dataset")
    # create object transformer
    transformer = Transformation(input_path)
    # apply transformation
    final_dataset = transformer.apply_filters()
    print("transformation is done")
    # load
    transformer.save_dataframe_to_csv(final_dataset, output_path)
