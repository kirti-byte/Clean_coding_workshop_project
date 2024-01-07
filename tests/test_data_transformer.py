import unittest
import os
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


def is_date_column(column):
    try:
        pd.to_datetime(column)
        return True
    except ValueError:
        return False


class TestDataCleaner(unittest.TestCase):

    def testFileExists(self):
        self.assertTrue(os.path.exists(processed_file_path),
                        "Processed File exists")

    def testNullValues(self):
        processed_df = pd.read_csv(processed_file_path)
        is_null_val = processed_df.isnull().values.sum()
        self.assertGreater(is_null_val, 0, "Null values are present")

    def testColCount(self):
        processed_df = pd.read_csv(processed_file_path)
        num_cols = len(processed_df.axes[1])
        self.assertEqual(num_cols, 27)

    def testDateColumnType(self):
        processed_df = pd.read_csv(processed_file_path)
        columnToCheck = 'date'
        self.assertTrue(is_date_column(processed_df[columnToCheck]))

    def test_dataframe_schema(self):

        processed_df = pd.read_csv(processed_file_path,
                                   dtype={'station_id': str, 'FRSHTT': str})
        expected_columns = [
            "station_id", "WBAN", "date", "temperature", "temperature_count",
            "dew_point", "dew_point_count", "sea_level_pressure",
            "sea_level_pressure_count",  "station_pressure",
            "station_pressure_count", "visibility", "visibility_count",
            "wind_speed", "wind_speed_count", "max_wind_speed", "GUST",
            "MAX", "MIN", "PRCP", "SNDP", "Fog", "Rain", "Snow",
            "Hail", "Thunder", "Tornado"
        ]
        expected_dtypes = ['object', 'int64', 'object', 'float64',
                           'int64', 'float64', 'int64', 'float64',
                           'int64', 'float64', 'int64',
                           'float64', 'int64', 'float64',
                           'int64', 'float64', 'float64', 'float64',
                           'float64', 'object', 'float64', 'int64',
                           'int64', 'int64', 'int64', 'int64',
                           'int64']
        # Check if the columns are present in the DataFrame
        self.assertListEqual(list(processed_df.columns), expected_columns)

        # Check if the data types of columns match the expected data types
        for col, expected_dtype in zip(expected_columns, expected_dtypes):
            self.assertEqual(processed_df[col].dtype, expected_dtype)


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
