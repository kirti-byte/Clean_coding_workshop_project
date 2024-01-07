import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
import logging

# Configure logging
logging.basicConfig(filename='weather_data_analysis.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_weather_data(file_path, rows=5000):
    """
    Read weather data from a CSV file.

    Args:
        file_path (str): Path to the weather data CSV file.
        rows (int): Number of rows to read from the file.

    Returns:
        pd.DataFrame: Weather data.
    """
    try:
        df = pd.read_csv(file_path).tail(rows)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"Error reading weather data: {e}")
        raise


def plot_min_max_temperature(df):
    """
    Plot Min and Max temperature by Station and Year-Month.

    Args:
        df (pd.DataFrame): Weather data.

    Returns:
        None
    """
    try:
        df['year_month'] = df['date'].dt.strftime("%Y-%m")
        df_long = pd.melt(df, id_vars=['station_id', 'year_month'],
                          value_vars=['MIN', 'MAX'], var_name='Temp_Type',
                          value_name='Temperature')

        # Plotting
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df_long, x='year_month', y='Temperature',
                    hue='station_id', hue_order=df['station_id'].unique(),
                    palette='husl', alpha=0.7, ci=None)
        plt.title('Min and Max Temperature by Station and Year-Month')
        plt.xlabel('Year-Month')
        plt.ylabel('Temperature')
        plt.legend(title='Station ID', bbox_to_anchor=(1.05, 1),
                   loc='upper left')
        plt.show()
    except Exception as e:
        logger.error(f"Error plotting Min and Max temperature: {e}")
        raise


def plot_monthly_mean_temperature(df):
    """
    Plot Monthly Mean Temperature.

    Args:
        df (pd.DataFrame): Weather data.

    Returns:
        None
    """
    try:
        df['Month'] = pd.to_datetime(df['date']).dt.month
        monthly_mean_temp = df.groupby('Month')['temperature'].mean().reset_index()

        # set the graph size
        plt.figure(figsize=(15, 7))

        # subplot(row, columns, first plot)
        plt.subplot(2, 2, 1)
        sns.lineplot(x='Month', y='temperature', data=monthly_mean_temp, marker='o')
        plt.title('Monthly Mean Temperature')
        plt.xlabel('Month')
        plt.ylabel('Mean Temperature (°F)')
        plt.show()
    except Exception as e:
        logger.error(f"Error plotting Monthly Mean Temperature: {e}")
        raise


def plot_temperature_distribution(df):
    """
    Plot Temperature Distribution.

    Args:
        df (pd.DataFrame): Weather data.

    Returns:
        None
    """
    try:
        plt.figure(figsize=(15, 7))

        # Temperature Distribution
        plt.subplot(2, 2, 3)
        sns.histplot(df['temperature'], bins=20, kde=True, color='red')
        plt.title('Temperature Distribution')
        plt.xlabel('Temperature (°F)')
        plt.ylabel('Frequency')
        plt.subplots_adjust(hspace=0.5, wspace=0.5)
        plt.show()
    except Exception as e:
        logger.error(f"Error plotting Temperature Distribution: {e}")
        raise
