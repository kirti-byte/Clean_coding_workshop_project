import pandas as pd
import numpy as np
import os
import logging

# Configure logging
logging.basicConfig(filename='weather_data_pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_and_clean_gsod_data(raw_data_folder_path):
    """
    Read and clean GSOD data from files in the specified folder.

    Args:
        raw_data_folder_path (str): The path to the folder
        containing raw GSOD data files.

    Returns:
        pd.DataFrame: Combined and cleaned GSOD data.
    """
    columns = [
        "station_id", "WBAN", "date", "temperature", "temperature_count",
        "dew_point", "dew_point_count", "sea_level_pressure",
        "sea_level_pressure_count",
        "station_pressure", "station_pressure_count", "visibility",
        "visibility_count",
        "wind_speed", "wind_speed_count", "max_wind_speed", "GUST",
        "MAX", "MIN", "PRCP", "SNDP", "FRSHTT"
    ]
    combined_df = pd.DataFrame()
    try:
        for path, subdirs, files in os.walk(raw_data_folder_path):
            for name in files:
                # Read gzip file and create Pandas DataFrame
                csv_file_path = os.path.join(path, name)
                with open(csv_file_path, 'rt') as f:
                    df = pd.read_csv(f, header=None, names=columns,
                                     delim_whitespace=True, skiprows=1,
                                     dtype={'station_id': str, 'FRSHTT': str})
                    # Data Cleaning Steps
                    df = clean_gsod_data(df)
                    combined_df = combined_df._append(df, ignore_index=True)
        return combined_df
    except Exception as e:
        logger.error(f"Error in read_and_clean_gsod_data: {e}")
        raise


def clean_gsod_data(df):
    """
    Clean GSOD data.

    Args:
        df (pd.DataFrame): GSOD data.

    Returns:
        pd.DataFrame: Cleaned GSOD data.
    """
    try:
        # 1. Remove asterisks (*) from columns
        df.replace(to_replace='\*', value='', regex=True, inplace=True)
        # 2. Convert columns to appropriate data types
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        numeric_columns = ['temperature', 'dew_point',
                           'sea_level_pressure', 'station_pressure',
                           'visibility', 'wind_speed', 'max_wind_speed',
                           'GUST', 'MAX', 'MIN', 'SNDP']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        # 3. Replace missing values (999.9, 99.99, etc.) with NaN
        df.replace(to_replace=[999.9, 99.99], value=np.nan, inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error in clean_gsod_data: {e}")
        raise


def write_clean_data_to_csv(df, clean_data_file_path, incremental):
    """
    Write cleaned GSOD data to a CSV file.

    Args:
        df (pd.DataFrame): Cleaned GSOD data.
        clean_data_file_path (str): The path to the CSV file.
        incremental (bool): Whether to perform an incremental write.

    Returns:
        None
    """
    try:
        if incremental:
            yearlist = list(df['date'].dt.year.unique())
            df_cleaned = pd.read_csv(clean_data_file_path)
            df_cleaned['date'] = pd.to_datetime(df_cleaned['date'])
            df_filtered = df_cleaned[~df_cleaned['date'].dt.year.isin(yearlist)]
            final_data = df_filtered._append(df)
            final_data.to_csv(clean_data_file_path, index=False)
        else:
            df.to_csv(clean_data_file_path, index=False)
        logger.info(f"Cleaned data written to {clean_data_file_path}")
    except Exception as e:
        logger.error(f"Error in write_clean_data_to_csv: {e}")
        raise
