"""
Process the weather data
"""


import os

from pyspark.sql import SparkSession
from datetime import datetime
from utils import setup_logger, now, base_dir

# Set up logger
logger = setup_logger("weather_processing.log")


class BaseProcessor:
    """
    Initialize class for processing weather stations data.
    """
    def __init__(self):    
        # Initialize the Spark session
        self.spark  = SparkSession.builder \
            .appName("WeatherDataProcessing") \
            .getOrCreate()
        
    def read_data(self, dir: str):
        """
        Read the csv files from the specified directory.
        
        :param dir: Directory containing the CSV files.
        """
        logger.info(f"Reading data from {dir}")
        
        # Read all CSV files in the directory
        df = self.spark.read.csv(dir, header=True, inferSchema=True)
        
        # Log the number of records read
        logger.info(f"Number of records read: {df.count()}")
        
        return df
    
    