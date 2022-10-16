import os

from pyspark.sql import SparkSession

from transformer.Transformer import Transformer

print("Starting Spark Job")

spark = SparkSession \
    .builder \
    .appName("My Awesome App") \
    .getOrCreate()

job_parameters = {
    "input_path": os.getenv("input_path"),
    "output_path": os.getenv("output_path"),
    "year": os.getenv("year"),
}

Transformer(spark, job_parameters).transform()

print("Spark Job Complete")
