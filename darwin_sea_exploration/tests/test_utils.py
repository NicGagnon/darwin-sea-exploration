from darwin_sea_exploration.main import *
from chispa.dataframe_comparer import assert_df_equality
import pytest

spark = SparkSession.builder.appName('testing').getOrCreate()


def test_transformation(sample_data, input_schema, output_schema):
    expected_value = [
        {"event": "registration_initiated", "timestamp": "2018-02-03 18:28:06.561000", "count": 1},
        {"event": "submission_success", "timestamp": "2018-01-30 18:13:43.627000", "count": 1}
    ]
    expected_df = spark.createDataFrame(data=expected_value, schema=output_schema)

    event_df = spark.createDataFrame(data=sample_data, schema=input_schema)
    actual_df = perform_transformations(event_df)

    assert_df_equality(actual_df, expected_df)