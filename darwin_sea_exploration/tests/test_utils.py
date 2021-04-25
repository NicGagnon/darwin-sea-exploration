# Local Imports
from darwin_sea_exploration.main import *

# Pyspark testing library
from chispa.dataframe_comparer import assert_df_equality

# Third Party libraries
from datetime import datetime
from os import listdir
import pandas as pd
import pytest

# Testing Session for unittest
spark = SparkSession.builder.appName('testing').getOrCreate()


def test_transformation(sample_data, input_schema, output_schema):
    expected_value = [
        {"event": "submission_success", "date": datetime.strptime("2018-01-30", '%Y-%m-%d'), "count": 1},
        {"event": "registration_initiated", "date": datetime.strptime("2018-02-03", '%Y-%m-%d'), "count": 1}
    ]
    df = pd.DataFrame(expected_value, columns=['event', 'date', 'count'])
    expected_df = spark.createDataFrame(data=df, schema=output_schema)

    event_df = spark.createDataFrame(data=sample_data, schema=input_schema)
    actual_df = perform_transformations(event_df)
    assert_df_equality(actual_df, expected_df)


def test_transformation_multiple_count(double_count_data, input_schema, output_schema):
    expected_value = [
        {"event": "submission_success", "date": datetime.strptime("2018-01-30", '%Y-%m-%d'), "count": 2}
    ]
    df = pd.DataFrame(expected_value, columns=['event', 'date', 'count'])
    expected_df = spark.createDataFrame(data=df, schema=output_schema)

    event_df = spark.createDataFrame(data=double_count_data, schema=input_schema)
    actual_df = perform_transformations(event_df)

    assert_df_equality(actual_df, expected_df)


def test_transformation_multiple_count_diff_date(multi_diff_count, input_schema, output_schema):
    expected_value = [
        {"event": "submission_success", "date": datetime.strptime("2018-01-30", '%Y-%m-%d'), "count": 2},
        {"event": "submission_success", "date": datetime.strptime("2018-01-31", '%Y-%m-%d'), "count": 3},
        {"event": "registration_initiated", "date": datetime.strptime("2018-01-31", '%Y-%m-%d'), "count": 1},
    ]
    df = pd.DataFrame(expected_value, columns=['event', 'date', 'count'])
    expected_df = spark.createDataFrame(data=df, schema=output_schema)

    event_df = spark.createDataFrame(data=multi_diff_count, schema=input_schema)
    actual_df = perform_transformations(event_df)

    assert_df_equality(actual_df, expected_df)


def test_missing_load_data():
    expected_value = []
    expected_df = spark.createDataFrame(data=expected_value, schema=get_schema())

    folder_path = Path(__file__).parent.joinpath('resources')
    actual_df = load_data(folder_path, spark, filename="fakename.json")

    assert_df_equality(actual_df, expected_df)


def test_sample_load_data(sample_data):
    expected_df = spark.createDataFrame(data=sample_data, schema=get_schema())

    folder_path = Path(__file__).parent.joinpath('resources')
    actual_df = load_data(folder_path, spark, filename="sample_data.json")

    assert_df_equality(actual_df, expected_df)


def test_transfer_data(sample_data):
    folder_path = Path(__file__).parent.joinpath('resources')
    output_path = folder_path.joinpath('output')
    transfer_valid_files(folder_path.joinpath("sample_data.json"), output_path)
    assert folder_path.joinpath("output").exists()
    num_files = len(listdir(folder_path.joinpath("output")))
    assert num_files == 2


def test_transfer_missing_data():
    folder_path = Path(__file__).parent.joinpath('resources')
    output_path = folder_path.joinpath('output')
    transfer_valid_files(folder_path.joinpath("sample_missing_data.json"), output_path)
    assert folder_path.joinpath("output").exists()
    num_files = len(listdir(folder_path.joinpath("output")))
    assert num_files == 0


def test_transfer_null_data(sample_data):
    folder_path = Path(__file__).parent.joinpath('resources')
    output_path = folder_path.joinpath('output')
    transfer_valid_files(folder_path.joinpath("sample_null_data.json"), output_path)
    assert folder_path.joinpath("output").exists()
    num_files = len(listdir(folder_path.joinpath("output")))
    assert num_files == 1
