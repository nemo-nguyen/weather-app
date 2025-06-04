"""
Process the weather data
"""


import os
import pyarrow as pa
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, instr, length, substring
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from datetime import datetime
from utils import setup_logger, now, base_dir

# Set up logger
logger = setup_logger("weather_processing.log")


class BaseProcessor:
    """
    Initialize class for processing weather stations data.
    """
    def __init__(self, dir):    
        # Initialize the Spark session
        self.dir = dir
        self.spark  = SparkSession.builder \
            .appName("WeatherDataProcessing") \
            .getOrCreate()
        self.conn = duckdb.connect(
            database=os.path.join(base_dir,"../db/weather.db")
            )
        self.df = self._read_from_dir()
        
        
    def _read_from_dir(self):
        """
        Read the csv files from the specified directory.
        
        :param dir: Directory containing the CSV files.
        """
        logger.info(f"Reading data from {self.dir}")
        
        # Read all CSV files in the directory
        df = self.spark.read.csv(self.dir, header=True, inferSchema=True)
        
        # Log the number of records read
        logger.info(f"Number of records read: {df.count()}")

        return df
    
    def _load_to_db(self, table_name, mode):
        pass
        

    # def extract_schema_str(self) -> str:
    #     """
    #     Generates a comma-separated schema string for table creation statements
    #     """
    #     return ",".join([f"{name} {type}" for name, type in self.df.dtypes()])


    
class StationDataProcessor(BaseProcessor): 
    def __init__(self, dir):
        super().__init__(dir)
        self.df = self.df.select(
            col("stationIdentifier").alias("id"),
            col("name"), 
            substring(
                col("timeZone"), 
                instr(col("timeZone"), "/") + 1, 
                length(col("timeZone"))
            ).alias("city"),
            col("ingestionDatetime")
        )
    
class ObservationDataProcessor(BaseProcessor):
    def __init__(self, dir):
        super().__init__(dir)
        self.elevation_schema = StructType([
            StructField("unitCode", StringType()),
            StructField("value", DoubleType())
        ])
        self.df = self.df.select(
            col("station").substr(-5,length(col("station"))).alias("stationId"),
            from_json(col("elevation"), self.elevation_schema).getField("value").alias("elevation_m")

        )
            

def example():
    station_proc = StationDataProcessor(dir = "/home/nemo/.projects/weather-app/data/stations")
    print(station_proc.df.show())

if __name__ == "__main__":
    example()
        

    
