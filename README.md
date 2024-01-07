# Weather Data Pipeline

## Overview

The Weather Data Pipeline is a project designed to download, clean, process,plot and archive historical weather data from the Global Surface Summary of the Day (GSOD) API provided by the National Climatic Data Center (NCDC).

## Features

- **Data Download:** Retrieve weather data files from the GSOD API.
- **Data Cleaning:** Process and clean the raw weather data for further analysis.
- **Data Processing:** Generate a final dataset from the cleaned data for various weather-related analyses.
- **Data Archiving:** Archive processed data files for future reference.

## Project Structure

The project is organized as follows:

- `src/`: Contains the source code for the data pipeline.
- `data/`:
  - `raw/`: Stores the raw weather data files downloaded from the GSOD API.
  - `processed/`: Contains the cleaned and final processed weather data files.
  - `archive/`: Archives processed data files for historical reference.

## Setup

1. Clone the repository:

```bash
git clone https://github.com/kirti-byte/Clean_coding_workshop_project.git
cd weather-data-pipeline
