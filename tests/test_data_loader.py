import unittest
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


class TestDataLoader(unittest.TestCase):
    def testApiCall(self):
        response = requests.get(base_url)
        self.assertEqual(response.status_code, 200, 'API did return 200')
        self.assertTrue(data_loader.download_file(base_url), "API call return did not response")

    def testFileCount(self):
        current_year = datetime.now().year
        expectedNumFiles = config.NUM_FILES * (current_year - 1929)
        actual_files = 0
        for path, subdirs, files in os.walk(archive_folder_path):
            actual_files += len(files)
        self.assertEqual(expectedNumFiles, actual_files+1, "File count is not matching")


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
