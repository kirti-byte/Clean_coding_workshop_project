import os
"""
This module defines configuration constants
and flags for a weather data pipeline.
It includes base URLs, folder paths, and
file paths necessary for downloading,
processing, and archiving weather data.
"""
"""
INCREMENTAL:
A boolean flag indicating whether incremental processing is enabled.
If both the cleaned and processed files exist, INCREMENTAL is set to True;
otherwise, it is set to False.
"""

# Base URL for the weather data API
BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/gsod/"

# Folder path for storing raw weather data files
RAW_FOLDER_PATH = "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/data/raw/"

# Number of weather data files to download
NUM_FILES = 4

# File path for storing cleaned weather data
CLEANED_FILE_PATH = "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/data/processed/clean_data.csv"

# File path for storing final processed weather data
PROCESSED_FILE_PATH = "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/data/processed/final_data.csv"

# Folder path for archiving processed data files
ARCHIVE_FOLDER_PATH = "C:/Users/sg1389-dsk01-user1/Documents/Workshop/weather-data-pipeline/data/archive/"

# Check if cleaned and processed files exist for incremental processing
if os.path.exists(CLEANED_FILE_PATH) and os.path.exists(PROCESSED_FILE_PATH):
    # If both files exist, set incremental processing flag to True
    INCREMENTAL = True
else:
    # If either of the files does not exist, set incremental processing flag to False
    INCREMENTAL = False
