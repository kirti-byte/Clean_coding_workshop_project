from behave import given, when, then
import requests
import os
from datetime import datetime
import sys
sys.path.insert(0, "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/src/")
import config as config
import data_loader


base_url = config.BASE_URL
num_files = config.NUM_FILES
raw_folder_path = config.RAW_FOLDER_PATH
processed_file_path = config.PROCESSED_FILE_PATH
incremental = config.INCREMENTAL
archive_folder_path = config.ARCHIVE_FOLDER_PATH


@given("the API url is configured")
def step_given_api_url(context):
    context.base_url = base_url


@when("an API is called")
def step_call_api(context):
    context.response = requests.get(base_url)


@then("the response status code should be 200")
def step_response_status_code(context):
    assert context.response.status_code == 200, 'API did not return 200'


@then("the API call should returns a response")
def step_api_return_response(context):
    assert data_loader.download_file(base_url), 'API did not return response'


@given("the archive folder path is configured")
def step_get_archive_file_path(context):
    context.archive_path = archive_folder_path
    context.num_files = num_files


@when("I check the file count in archive folder")
def step_archive_file_count_check(context):
    current_year = datetime.now().year
    context.expected_num_files = num_files * (current_year - 1929)
    context.actual_files = 0
    for dir, subdir, files in os.walk(context.archive_path):
        context.actual_files += len(files)


@then("archive file count should match with the expected file count")
def step_file_count_matches_expected(context):
    assert context.expected_num_files == context.actual_files+1, 'File count is not matching'
