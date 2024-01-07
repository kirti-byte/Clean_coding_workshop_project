import pandas as pd
import os
import shutil
import logging

# Configure logging
logging.basicConfig(filename='data_processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_and_transform_gsod_files(clean_data_file_path):
    """
    Read cleaned GSOD data from a CSV file and transform
    it by splitting the FRSHTT column.

    Args:
        clean_data_file_path (str): Path to the cleaned GSOD data CSV file.

    Returns:
        pd.DataFrame: Transformed GSOD data.
    """
    try:
        df = pd.read_csv(clean_data_file_path, 
                         dtype={'station_id': str, 'FRSHTT': str})
        df_transform = split_frshtt_column(df)
        return df_transform
    except Exception as e:
        logger.error(f"Error in read_and_transform_gsod_files: {e}")
        raise


def split_frshtt_column(df):
    """
    Split the FRSHTT column into separate columns
    for each weather condition.

    Args:
        df (pd.DataFrame): GSOD data.

    Returns:
        pd.DataFrame: Transformed GSOD data.
    """
    try:
        df['Fog'] = df['FRSHTT'].str[0].astype(int)
        df['Rain'] = df['FRSHTT'].str[1].astype(int)
        df['Snow'] = df['FRSHTT'].str[2].astype(int)
        df['Hail'] = df['FRSHTT'].str[3].astype(int)
        df['Thunder'] = df['FRSHTT'].str[4].astype(int)
        df['Tornado'] = df['FRSHTT'].str[5].astype(int)
        df.drop(columns=['FRSHTT'], inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error in split_frshtt_column: {e}")
        raise


def write_final_data_to_csv(df, processed_file_path, incremental):
    """
    Write the final processed GSOD data to a CSV file.

    Args:
        df (pd.DataFrame): Final processed GSOD data.
        processed_file_path (str): Path to the processed GSOD data CSV file.
        incremental (bool): Whether to perform an incremental write.

    Returns:
        None
    """
    try:
        if incremental:
            df['date'] = pd.to_datetime(df['date'])
            yearlist = list(df['date'].dt.year.unique())
            df_processed = pd.read_csv(processed_file_path)
            df_processed['date'] = pd.to_datetime(df_processed['date'])
            df_filtered = df_processed[~df_processed['date'].dt.year.isin(yearlist)]
            final_data = df_filtered._append(df)
            final_data.to_csv(processed_file_path, index=False)
        else:
            df.to_csv(processed_file_path, index=False)
        logger.info(f"Processed data written to {processed_file_path}")
    except Exception as e:
        logger.error(f"Error in write_final_data_to_csv: {e}")
        raise


def move_raw_data_to_archive(raw_data_path, archive_raw_data_path):
    """
    Move raw data from the source folder to the destination archive folder.

    Args:
        raw_data_path (str): Source path of raw data.
        archive_raw_data_path (str): Destination path of archived raw data.

    Returns:
        None
    """
    try:
        files_and_folders = os.listdir(raw_data_path)
        for item in files_and_folders:
            source_item_path = os.path.join(raw_data_path, item)
            destination_item_path = os.path.join(archive_raw_data_path, item)
            if os.path.exists(destination_item_path):
                shutil.rmtree(destination_item_path)
            shutil.move(source_item_path, archive_raw_data_path)
            logger.info(f"Moved {item} to {archive_raw_data_path}")
    except Exception as e:
        logger.error(f"Error in move_raw_data_to_archive: {e}")
        raise
