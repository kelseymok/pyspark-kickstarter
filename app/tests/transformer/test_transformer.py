import os
import shutil
import pandas as pd
import pytest
import sys

from pyspark.sql.types import StringType, StructType, FloatType, StructField, IntegerType

from test_utils.pyspark import TestPySpark
from transformer.Transformer import Transformer


class TesttTransformer(TestPySpark):

    @classmethod
    def setup_class(cls):
        cls.spark = cls.start_spark()
        root_dir = os.path.dirname(os.path.realpath(__file__)).split("/transformer/")[0]
        cls.parameters = {
            "input_path": f"{root_dir}/tmp_input/",
            "output_path": f"{root_dir}/tmp_output/",
            "year": "2022",
        }
        cls.transformer = Transformer(cls.spark, cls.parameters)
        return

    @classmethod
    def teardown_class(cls):
        cls.stop_spark()
        shutil.rmtree(cls.parameters["output_path"], ignore_errors=True)
        shutil.rmtree(cls.parameters["input_path"], ignore_errors=True)


    def test_transform(self):
        input_pandas = pd.DataFrame(
            [
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2021-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'kWh',
                    'value': 15.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2021-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'Wh',
                    'value': 650.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'kWh',
                    'value': 15.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'Wh',
                    'value': 650.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 176,
                    'unit': 'kWh',
                    'value': 12.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 176,
                    'unit': 'Wh',
                    'value': 500.0
                }
            ]

        )
        input_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("context", StringType(), True),
            StructField("format", StringType(), True),
            StructField("location", StringType(), True),
            StructField("measurand", StringType(), True),
            StructField("phase", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("value", FloatType(), True),
        ])

        input_df = self.spark.createDataFrame(input_pandas, input_schema)

        input_df.write.mode("overwrite").parquet(self.transformer.parameters["input_path"])

        self.transformer.transform()

        output = os.listdir(f'{self.transformer.parameters["output_path"]}')
        assert len(list(filter(lambda f: f.endswith(".csv"), output))) == 1
        assert len(list(filter(lambda f: f.endswith("_SUCCESS"), output))) == 1


    def test_sum_energy_trasnfer(self):
        input_pandas = pd.DataFrame(
            [
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'kWh',
                    'value': 15.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'Wh',
                    'value': 650.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 176,
                    'unit': 'kWh',
                    'value': 12.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 176,
                    'unit': 'Wh',
                    'value': 500.0
                }
            ]

        )
        input_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("context", StringType(), True),
            StructField("format", StringType(), True),
            StructField("location", StringType(), True),
            StructField("measurand", StringType(), True),
            StructField("phase", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("value", FloatType(), True),
        ])

        df = self.spark.createDataFrame(input_pandas, input_schema)

        result = df.transform(self.transformer._filter_energy_transfer)
        pandas_df = result.toPandas().to_dict(orient="records")
        assert pandas_df == [
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Energy.Active.Import.Register',
                'phase': 'L1',
                'timestamp': '2022-10-15T07:08:06.186748+00:00',
                'transaction_id': 175,
                'unit': 'Wh',
                'value': 650.0
            },
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Energy.Active.Import.Register',
                'phase': 'L1',
                'timestamp': '2022-10-15T07:08:06.186748+00:00',
                'transaction_id': 176,
                'unit': 'Wh',
                'value': 500.0
            }
        ]


    def test_select_year(self):
        input_pandas = pd.DataFrame(
            [
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2021-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'kWh',
                    'value': 15.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Energy.Active.Import.Register',
                    'phase': 'L1',
                    'timestamp': '2021-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'Wh',
                    'value': 650.0
                },
                {
                    'connector_id': 1,
                    'context': 'Sample.Periodic',
                    'format': 'Raw',
                    'location': 'Outlet',
                    'measurand': 'Power.Active.Import',
                    'phase': 'L1',
                    'timestamp': '2022-10-15T07:08:06.186748+00:00',
                    'transaction_id': 175,
                    'unit': 'kWh',
                    'value': 15.0
                },
            ]

        )

        input_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("context", StringType(), True),
            StructField("format", StringType(), True),
            StructField("location", StringType(), True),
            StructField("measurand", StringType(), True),
            StructField("phase", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("value", FloatType(), True),
        ])

        df = self.spark.createDataFrame(input_pandas, input_schema)

        result = df.transform(self.transformer._select_year(2021))
        pandas_df = result.toPandas().to_dict(orient="records")
        assert pandas_df == [
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Power.Active.Import',
                'phase': 'L1',
                'timestamp': '2021-10-15T07:08:06.186748+00:00',
                'transaction_id': 175,
                'unit': 'kWh',
                'value': 15.0
            },
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Energy.Active.Import.Register',
                'phase': 'L1',
                'timestamp': '2021-10-15T07:08:06.186748+00:00',
                'transaction_id': 175,
                'unit': 'Wh',
                'value': 650.0
            },
        ]

    def test_convert(self):
        input_pandas = pd.DataFrame([
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Power.Active.Import',
                'phase': 'L1',
                'timestamp': '2021-10-15T07:08:06.186748+00:00',
                'transaction_id': 175,
                'unit': 'kWh',
                'value': 15.0
            },
            {
                'connector_id': 1,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': 'Outlet',
                'measurand': 'Energy.Active.Import.Register',
                'phase': 'L1',
                'timestamp': '2021-10-15T07:08:06.186748+00:00',
                'transaction_id': 175,
                'unit': 'Wh',
                'value': 650.0
            },
        ])
        input_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("context", StringType(), True),
            StructField("format", StringType(), True),
            StructField("location", StringType(), True),
            StructField("measurand", StringType(), True),
            StructField("phase", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("value", FloatType(), True),
        ])
        df = self.spark.createDataFrame(input_pandas, input_schema)

        self.transformer._convert(df, output_path=self.transformer.parameters["output_path"])
        output = os.listdir(f'{self.transformer.parameters["output_path"]}/')
        assert len(list(filter(lambda f: f.endswith(".csv"), output))) == 1
        assert len(list(filter(lambda f: f.endswith("_SUCCESS"), output))) == 1


if __name__ == "__main__":
    pytest.main(sys.argv)
