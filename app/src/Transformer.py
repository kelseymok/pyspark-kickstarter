from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F

class Transformer:
    def __init__(self, spark: SparkSession, parameters: "dict[str, str]"):
        self.spark = spark
        self.parameters = parameters
        return

    def transform(self):
        input_df = self._read(f"{self.parameters['input_path']}/")
        df = input_df.\
            transform(self._select_year(year=int(self.parameters["year"]))).\
            transform(self._filter_energy_transfer)
        self._convert(df, output_path=self.parameters["output_path"])

    def _filter_energy_transfer(self, df: DataFrame) -> DataFrame:
        return df.filter(F.col("measurand") == F.lit("Energy.Active.Import.Register"))

    def _select_year(self, year: int):
        def inner(df: DataFrame) -> DataFrame:
            df = df.\
                    withColumn("year", F.year(F.to_timestamp("timestamp"))).\
                filter(F.col("year") == F.lit(year)).\
                drop("year")
            return df
        return inner

    def _convert(self, df: DataFrame, output_path):
        df.write.option("header", True).mode("overwrite").csv(f"{output_path}/")

    def _read(self, path):
        return self.spark.read.load(path)