'''
# Author Information
Author : Kirti Jain
Last Modified : 7th Jan 2024

# Project Summary

This script serves as the main entry point
for the weather data processing pipeline.
It downloads, cleans, transforms, archives, 
and analyzes weather data, followed by plotting charts.

'''

import logging
import config
from data_loader import download_files_parallel
from data_transformer import (
    read_and_transform_gsod_files,
    write_final_data_to_csv,
    move_raw_data_to_archive
)
from data_analyzer import (read_weather_data,
                           plot_min_max_temperature,
                           plot_monthly_mean_temperature,
                           plot_temperature_distribution)
from data_cleaner import read_and_clean_gsod_data, write_clean_data_to_csv


# Configure logging
logging.basicConfig(filename='weather_data_pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Main function to orchestrate the execution of data processing modules.

    Downloads data, cleans, transforms, archives, analyze and plot chart.

    Returns:
        None
    """
    try:
        # Download data in parallel
        download_files_parallel(base_url=config.BASE_URL,
                                raw_folder_path=config.RAW_FOLDER_PATH,
                                num_files=config.NUM_FILES,
                                incremental=config.INCREMENTAL)

        # Clean data
        clean_data = read_and_clean_gsod_data(raw_data_folder_path=
                                              config.RAW_FOLDER_PATH)
        write_clean_data_to_csv(clean_data,
                                clean_data_file_path=config.CLEANED_FILE_PATH,
                                incremental=config.INCREMENTAL)

        # Transform data
        transformed_data = read_and_transform_gsod_files(
            clean_data_file_path=config.CLEANED_FILE_PATH
        )
        write_final_data_to_csv(transformed_data,
                                processed_file_path=config.PROCESSED_FILE_PATH,
                                incremental=config.INCREMENTAL)

        # Archive raw data
        move_raw_data_to_archive(raw_data_path=config.RAW_FOLDER_PATH,
                                 archive_raw_data_path=
                                 config.ARCHIVE_FOLDER_PATH)

        # Analyze and plot chart
        df = read_weather_data(config.PROCESSED_FILE_PATH)
        # Plot Monthly Mean Temperature
        plot_monthly_mean_temperature(df)
        # Plot Temperature Distribution
        plot_temperature_distribution(df)
        # Plot Min and Max Temperature
        plot_min_max_temperature(df)

        logger.info("Weather Data Processing Completed")

    except Exception as e:
        logger.error(f"Error in main script: {e}")
        raise


if __name__ == "__main__":
    main()
