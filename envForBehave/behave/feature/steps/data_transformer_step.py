import os
from datetime import datetime
import sys
sys.path.insert(0, "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/src/")
import config as config
import pandas as pd
from behave import given, when, then


base_url = config.BASE_URL
num_files = config.NUM_FILES
raw_folder_path = config.RAW_FOLDER_PATH
processed_file_path = config.PROCESSED_FILE_PATH
cleaned_file_path = config.CLEANED_FILE_PATH
incremental = config.INCREMENTAL
archive_folder_path = config.ARCHIVE_FOLDER_PATH


@given("the transformed file path is configured")
def step_given_processed_file_path(context):
    context.processed_file_path = processed_file_path


@when("I read data from transformed data file")
def step_check_processed_file_exist(context):
    context.file_exist = os.path.exists(context.processed_file_path)


@then("transformed file should exist")
def step_processed_file_exists(context):
    assert context.file_exist, 'Processed data file does not exists'


@given('the transformed file is loaded into a DataFrame')
def step_given_load_processed_file(context):
    context.processed_df = pd.read_csv(processed_file_path, dtype={'station_id':'str'})


@when('I check the number of transformed columns')
def step_when_check_column_count(context):
    context.num_cols = len(context.processed_df.axes[1])


@then('the number of columns should be 27')
def step_then_column_count_correct(context):
    assert context.num_cols == 27, 'Number of columns is not 27'


@when('I check the transformed DataFrame schema')
def step_when_check_dataframe_schema(context):
    context.expected_columns = [
            "station_id", "WBAN", "date", "temperature", "temperature_count",
            "dew_point", "dew_point_count", "sea_level_pressure", "sea_level_pressure_count",
            "station_pressure", "station_pressure_count", "visibility", "visibility_count",
            "wind_speed", "wind_speed_count", "max_wind_speed", "GUST",
            "MAX", "MIN", "PRCP", "SNDP", "Fog", "Rain", "Snow","Hail", "Thunder", "Tornado"
        ]
    context.expected_dtypes = ['object', 'int64', 'object', 'float64', 'int64', 'float64',
            'int64', 'float64', 'int64', 'float64', 'int64', 'float64', 'int64', 'float64',
            'int64', 'float64', 'float64', 'float64', 'float64', 'object', 'float64',
            'int64', 'int64', 'int64', 'int64', 'int64', 'int64']


@then('the transformed DataFrame schema should match the expected columns and data types')
def step_then_dataframe_schema_correct(context):
    assert list(context.processed_df.columns) == context.expected_columns, 'Columns do not match expected columns'
    for col, expected_dtype in zip(context.expected_columns, context.expected_dtypes):
        assert context.processed_df[col].dtype == expected_dtype, f"Data type for {col} does not match expected data type"
