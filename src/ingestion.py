"""
Crawl data from NOAA API endpoint
""" 

import requests
import os
import csv
import datetime
import logging

# Set up logging
base_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(base_dir, "../logs")
os.makedirs(logs_dir, exist_ok=True)  

# Path to the log file
log_file_path = os.path.join(logs_dir, "weather_ingestion.log")  

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger(__name__)

class Ingestion:
    def __init__(self, params: dict = {"limit": 10}):
        """
        Initialize the base class for weather data ingestion.
        
        :param limit: The maximum number of records to retrieve (optional).
        """
        self.params = params
        self.base_url = "https://api.weather.gov/"
        self.endpoint = None
        self.headers = []
        self.data = []
    
    
    def fetch_data(self):
        """
        Get the data from the NOAA API.
        """
        logging.info(f"Fetching data from {self.base_url}")
        
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
            logging.info(f"Data fetched successfully from {self.base_url}")
        else:
            logging.error(f"Error: {response.status_code}, {response.reason}")

    
    def save_to_csv(self, filename, mode='w'):
        """
        Save the provided data to a CSV file.

        :param filename: The name of the output CSV file.
        :param mode: The mode in which to open the file ('w' for write, 'a' for append).
        """
        # Check if data is available
        if self.data == []:
            logging.debug("No data to save.")
            return

        # Check if headers are available
        if self.headers == []:
            logging.debug("No headers available.")
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
                values.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                writer.writerow(values)
        logging.info(f"Data saved to {file_path}")


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
            logging.debug("No data available to extract station IDs.")
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
            logging.error("Station ID is required for observations ingestion.")
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
        

def main():
    # Example usage
    
    station_data = StationsIngestion(params={"state": "CA", "limit": 1})
    station_ids = station_data.get_station_id()
    # print(station_ids)

    observation_data = ObservationsIngestion(station_id=station_ids, params={"start":"2025-05-02T04:00:00Z", "limit": 10})
    # print(observation_data.data)
    observation_data.save_to_csv("observations.csv")
    
if __name__ == "__main__":
    main()
