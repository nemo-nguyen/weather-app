"""
Crawl weather data from NOAA API endpoint
API specification can be found at https://www.weather.gov/documentation/services-web-api#/default/tafs
""" 

import requests
import os
import csv
import datetime
from utils import setup_logger, now, base_dir

# Set up logger
logger = setup_logger("weather_ingestion.log")

class Ingestion:
    def __init__(self, params: dict = {"limit": 10}):
        """
        Initialize the base class for weather data ingestion.
        
        :param limit: The maximum number of records to retrieve (optional).
        """
        # If limit is not provided, defaults to 10
        if params.get("limit") is None:
            params["limit"] = 10

        # Set the default parameters
        self.params = params
        self.base_url = "https://api.weather.gov/"
        self.endpoint = None
        self.headers = []
        self.data = []
    
    
    def fetch_data(self):
        """
        Get the data from the NOAA API.
        """
        logger.info(f"Fetching data from {self.base_url}")
        
        response = requests.get(
            self.base_url,
            params={k:v for (k,v) in self.params.items() if v is not None},
            )
        
        # Check if the request was successful
        if response.status_code == 200:
            # Insert fetched data into self.data
            response_feature = response.json().get('features', [])
            self.data.extend([data.get('properties', []) for data in response_feature])
            # Extract headers from the first data entry
            if self.data and not self.headers:
                self.headers = [headers for headers in self.data[0].keys()]
            # Log the successful data retrieval
            logger.info(f"Data fetched successfully from {self.base_url}")
        else:
            logger.error(f"Error: {response.status_code}, {response.reason}")

    
    def save_to_csv(self, filename, mode='w'):
        """
        Save the provided data to a CSV file.

        :param filename: The name of the output CSV file.
        :param mode: The mode in which to open the file ('w' for write, 'a' for append).
        """
        # Check if data is available
        if self.data == []:
            logger.debug("No data to save.")
            return

        # Check if headers are available
        if self.headers == []:
            logger.debug("No headers available.")
            return
        
        # Construct path to the output file
        output_dir = os.path.join(base_dir, "../data")
        os.makedirs(output_dir, exist_ok=True) 
        file_path = os.path.join(output_dir, filename)
        
        # Write to csv
        with open(file_path, mode=mode, newline='') as f:
            writer = csv.writer(f)
            # Include ingestion time
            self.headers.append("ingestionDatetime")
            # Write the headers
            writer.writerow(self.headers)
            # Write the data
            for row in self.data:
                # Extract the values from the row
                values = [row.get(header, '') for header in self.headers if header != "ingestionDatetime"]
                # Add ingestion time
                values.append(now.strftime("%Y-%m-%d %H:%M:%S"))
                writer.writerow(values)
        logger.info(f"Data saved to {file_path}")


class StationsIngestion(Ingestion):
    def __init__(self, params: dict = {"state": None, "limit": 10}):
        """
        Initialize the StationIngestion class for weather station data ingestion.

        :param state: The state code for the weather data (optional).
        :param limit: The maximum number of records to retrieve (optional).
        """
        
        # Call the base class constructor
        super().__init__(params)

        # Update the base URL with stations endpoint
        self.endpoint = "stations"
        self.base_url = "".join(["https://api.weather.gov/", self.endpoint])
        
        # Fetch the stations data  
        self.fetch_data()

    def get_station_id(self) -> list:
        """
        Get all station IDs from the data.
        """
        if self.data:
            return [station.get('stationIdentifier', None) for station in self.data]
        else:
            logger.debug("No data available to extract station IDs.")
            return None


class ObservationsIngestion(Ingestion):
    def __init__(self, station_id:str|list, params: dict = {"start": None, "end": None, "limit": 10}):
        """
        Initialize the ObservationsIngestion class for weather observations data ingestion.

        :param station_id: The station ID or list of station IDs for the weather data.
        :param state: The state code for the weather data (optional).
        :param limit: The maximum number of observation to retrieve for each station (optional).
        :param start: The start date for the observations, expects UTC format: YYYY-MM-DDThh:mm:ssZ (optional).
        :param end: The end date for the observations, expects UTC format: YYYY-MM-DDThh:mm:ssZ (optional).
        """
        
        # Call the base class constructor
        super().__init__(params)

        # Check if station_id is provided
        if not station_id:
            logger.error("Station ID is required for observations ingestion.")
            raise ValueError("Station ID is required for observations ingestion.")

        # Check if station_id is a list or a single value
        if isinstance(station_id, list):
            self.station_id = station_id
        else:
            self.station_id = [station_id]
        
        for station_id in self.station_id:
            # Update the base URL with the station ID
            self.endpoint = f"stations/{station_id}/observations"
            self.base_url = "".join(["https://api.weather.gov/", self.endpoint])
            
            # Fetch the observations data  
            self.fetch_data()
        

def example():
    """
    Example usages of the Ingestion classes.
    """
    # Getting stations data in California
    station_data = StationsIngestion(
        params={"state": "CA"}
        )
    station_data.save_to_csv("stations/stations.csv")
    
    # Setting the start and end time for the observations
    start = (now - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(hour=23, minute=59, second=59, microsecond=0)
   
   # Getting 100 observations of each station on the previous day
    observation_data = ObservationsIngestion(
        station_id = station_data.get_station_id(), 
        params={
            "start": start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "end": end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "limit": 100}
        )
    observation_data.save_to_csv(f"observations/observations_{start.strftime('%Y%m%d')}.csv")
    
if __name__ == "__main__":
    example()
