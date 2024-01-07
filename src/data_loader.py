import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import logging
from urllib.parse import urljoin
import gzip
from io import BytesIO

# Configure logging
logging.basicConfig(filename='logs_weather_data_pipeline', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_file(url):
    """
    Download file content from the given URL.

    Args:
        url (str): The URL of the file to be downloaded.

    Returns:
        bytes: The content of the downloaded file.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file from {url}: {e}")
        raise


def download_files_from_year(base_url, raw_folder_path, year_folder, num_files, incremental):
    """
    Download files for a specific year from the given base URL.

    Args:
        base_url (str): The base URL where year folders are located.
        raw_folder_path (str): The path to the raw data folder.
        year_folder (str): The specific year folder to download files from.
        num_files (int): The number of files to download.
        incremental (bool): Whether to perform an incremental load.

    Returns:
        None
    """
    year_url = urljoin(base_url, year_folder)

    try:
        response = requests.get(year_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract links to files
        file_links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.op.gz')]

        # Check if the folder is modified in the last day for incremental load
        if incremental:
            last_modified_element = soup.find_all('td', align='right')
            last_modified_date = last_modified_element[-2].get_text(strip=True)
            last_modified_date = datetime.strptime(last_modified_date, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d')
            yesterday = datetime.now().date() - timedelta(days=1)
            yesterday = yesterday.strftime('%Y-%m-%d')
            if last_modified_date < yesterday:
                logger.info(f"Skipping {year_folder} - No updates in the last day.")
                return

        # Download only the specified number of files
        file_links = file_links[:num_files]

        # Download files
        destination_folder = os.path.join(raw_folder_path, year_folder)
        os.makedirs(destination_folder, exist_ok=True)

        for file_link in file_links:
            file_url = year_url + '/' + file_link
            file_name = os.path.basename(file_link)
            destination_path = os.path.join(destination_folder, file_name.replace('.op.gz', '.op'))
            logger.info(f"Downloading {file_url} to {destination_path}")

            # Download and decompress the file
            file_content = download_file(file_url)
            with gzip.GzipFile(fileobj=BytesIO(file_content), mode='rb') as gz_file:
                decompressed_content = gz_file.read()

            # Save the decompressed content to a new file
            with open(destination_path, 'wb') as output_file:
                output_file.write(decompressed_content)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading files from {year_url}: {e}")
        raise


def download_files_parallel(base_url, raw_folder_path, num_files, incremental):
    """
    Download files for multiple years in parallel.

    Args:
        base_url (str): The base URL where year folders are located.
        raw_folder_path (str): The path to the raw data folder.
        num_files (int): The number of files to download for each year.
        incremental (bool): Whether to perform an incremental load.

    Returns:
        None
    """
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Extract links to year folders
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            year_folders = [a['href'] for a in soup.find_all('a') if a['href']]
            year_values = [value.replace('/', '') for value in year_folders 
                           if value.replace('/', '').isdigit()]

            # Download files for each year in parallel
            partial_download = lambda year: download_files_from_year(
                base_url, raw_folder_path, year, num_files, incremental)
            executor.map(partial_download, year_values)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading files in parallel: {e}")
        raise
