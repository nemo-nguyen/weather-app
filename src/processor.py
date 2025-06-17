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
    
    def _exist(self, table_name) -> bool: 
        """
        Check if a table exists in the database
        """
        return self.conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'", 
            ).fetchone()[0] > 0
            
    def _load_to_db(self, table_name, mode, truncate = False) -> None:
        """
        Load the processed data to a table in the database, supports 2 modes: a - append, o - overwrite
        """
        df_arrow = self.df.toArrow()
        self.conn.register(f"{table_name}_stg", df_arrow)
        if mode == "o":
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name}_stg")
        elif mode == "a":
            if self._exist(table_name):
                if truncate:
                    self.conn.execute(f"TRUNCATE TABLE {table_name}")
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_stg")
            else:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_stg")
        else:
            logger.error("Incorrect writing mode, only accepts 'a' or 'w'")

    
class StationDataProcessor(BaseProcessor): 
    def __init__(self, dir):
        super().__init__(dir)
        self.df = self.df.select(
            col("stationIdentifier").alias("id"),
            col("name"), 
            col("ingestionDatetime")
        )


class CountyDataProcessor(BaseProcessor):
    def __init__(self, dir):
        super().__init__(dir)
        self.df = self.df.select(
            col("id"),
            col("name"),
            col("state"),
            col("forecastOffice").substr(-3,3).alias("forecastOfficeId"),
            col("ingestionDatetime")
        )


class OfficeDataProcessor(BaseProcessor):
    def __init__(self, dir):
        super().__init__(dir)
        self.address_schema = StructType([
            StructField("@type", StringType()),
            StructField("streetAddress", StringType()),
            StructField("addressLocality", StringType()),
            StructField("addressRegion", StringType()),
            StructField("postalCode", StringType())
        ])
        self.df = self.df.select(
            col("id"),
            col("name"),
            from_json(col("address"), self.address_schema).getField("streetAddress").alias("streetAddress"),
            from_json(col("address"), self.address_schema).getField("addressLocality").alias("addressLocality"),
            from_json(col("address"), self.address_schema).getField("addressRegion").alias("addressRegion"),
            from_json(col("address"), self.address_schema).getField("postalCode").alias("postalCode"),
            col("ingestionDatetime")
        )


class ObservationDataProcessor(BaseProcessor):
    def __init__(self, dir):
        super().__init__(dir)
        self.elevation_schema = StructType([
            StructField("unitCode", StringType()),
            StructField("value", DoubleType())
        ])
        self.general_schema = StructType([
            StructField("unitCode", StringType()),
            StructField("value", DoubleType()),
            StructField("qualityControl", StringType())
        ]) 
        self.df = self.df.select(
            col("timestamp").alias("observationTime"),
            col("station").substr(-5, 5).alias("stationId"),
            from_json(col("elevation"), self.elevation_schema).getField("value").alias("elevationInMeter"),
            from_json(col("temperature"), self.general_schema).getField("value").alias("tempInDegC"),
            from_json(col("windSpeed"), self.general_schema).getField("value").alias("windSpeedInKmpH"),
            from_json(col("relativeHumidity"), self.general_schema).getField("value").alias("relHumidityInPct"),
            col("ingestionDatetime")
        )
            

def example():
    station_proc = StationDataProcessor(dir = "/home/nemo/.projects/weather-app/data/stations")
    station_proc._load_to_db(table_name="station", mode="o")
    print(station_proc.conn.execute("SELECT * FROM station").fetchall())

    county_proc = CountyDataProcessor(dir = "/home/nemo/.projects/weather-app/data/counties")
    county_proc._load_to_db(table_name="county", mode="o")
    print(county_proc.conn.execute("SELECT * FROM county").fetchall())

    office_proc = OfficeDataProcessor(dir = "/home/nemo/.projects/weather-app/data/offices")
    office_proc._load_to_db(table_name="office", mode="o")
    print(office_proc.conn.execute("SELECT * FROM office").fetchall())

    obs_proc = ObservationDataProcessor(dir="/home/nemo/.projects/weather-app/data/observations")
    obs_proc._load_to_db(table_name="observation", mode="a")
    print(obs_proc.conn.execute("SELECT * FROM observation").fetchall())

if __name__ == "__main__":
    example()
        

    
