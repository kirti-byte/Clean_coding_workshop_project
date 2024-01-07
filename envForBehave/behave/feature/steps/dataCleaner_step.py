from behave import given, when, then
import os
from datetime import datetime
import sys
sys.path.insert(0, "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/src/")
import config as config
import pandas as pd


base_url = config.BASE_URL
num_files = config.NUM_FILES
raw_folder_path = config.RAW_FOLDER_PATH
processed_file_path = config.PROCESSED_FILE_PATH
cleaned_file_path = config.CLEANED_FILE_PATH
incremental = config.INCREMENTAL
archive_folder_path = config.ARCHIVE_FOLDER_PATH


@given("the cleaned file path is configured")
def step_given_cleaned_path(context):
    context.cleaned_file_path = cleaned_file_path


@when("I read data from clean data file")
def step_check_clean_file_exist(context):
    context.file_exist = os.path.exists(context.cleaned_file_path)


@then("file should exist")
def step_clean_file_exists(context):
    assert context.file_exist, 'Clean data file does not exists'


@given('the cleaned file is loaded into a DataFrame')
def step_given_load_cleaned_file(context):
    context.cleaned_df = pd.read_csv(cleaned_file_path, dtype={'station_id': str, 'FRSHTT': str})


@when('I check the number of columns')
def step_when_check_column_count(context):
    context.num_cols = len(context.cleaned_df.axes[1])


@then('the number of columns should be 22')
def step_then_column_count_correct(context):
    assert context.num_cols == 22, 'Number of columns is not 22'


@when('I check the length of the {col_name} column')
def step_when_check_column_length(context, col_name):
    context.col_name = col_name
    context.len_of_col = context.cleaned_df[col_name].apply(lambda x: len(str(x)) == 6)


@then('all values in the {col_name} column should have a length of 6')
def step_then_column_length_correct(context, col_name):
    assert all(context.len_of_col), f"Values in the {col_name} column do not have a length of 6"


@when('I check the DataFrame schema')
def step_when_check_dataframe_schema(context):
    context.expected_columns = [
        "station_id", "WBAN", "date", "temperature", "temperature_count",
        "dew_point", "dew_point_count", "sea_level_pressure", "sea_level_pressure_count",
        "station_pressure", "station_pressure_count", "visibility", "visibility_count",
        "wind_speed", "wind_speed_count", "max_wind_speed", "GUST",
        "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"
    ]
    context.expected_dtypes = ['object', 'int64', 'object', 'float64', 'int64', 'float64',
                               'int64', 'float64', 'int64', 'float64', 'int64', 'float64', 'int64', 'float64',
                               'int64', 'float64', 'float64', 'float64', 'float64', 'object', 'float64', 'object']


@then('the DataFrame schema should match the expected columns and data types')
def step_then_dataframe_schema_correct(context):
    assert list(context.cleaned_df.columns) == context.expected_columns, 'Columns do not match expected columns'
    for col, expected_dtype in zip(context.expected_columns, context.expected_dtypes):
        assert context.cleaned_df[col].dtype == expected_dtype, f"Data type for {col} does not match expected data type"
