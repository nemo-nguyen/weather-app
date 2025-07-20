"""
Crawl weather data from NOAA API endpoint
API specification can be found at https://www.weather.gov/documentation/services-web-api#/default/tafs
""" 

import requests
import os
import csv
import datetime
from .utils import setup_logger, AIRFLOW_HOME, now

# Set up logger
logger = setup_logger("weather_ingestion.log")

class BaseIngestor:
    def __init__(self, params: dict = None):
        """
        Initialize the base class for weather data ingestion.
        
        :param limit: The maximum number of records to retrieve (optional).
        """
        # # If limit is not provided, defaults to 10
        # if params.get("limit") is None:
        #     params["limit"] = 10

        # Set the default parameters
        self.params = params
        self.base_url = "https://api.weather.gov/"
        self.endpoint = None
        self.headers = []
        self.data = []
    
    
    def _fetch_data(self):
        """
        Get the data from the NOAA API.
        """
        logger.info(f"Fetching data from {self.base_url}")
        
        response = requests.get(
            url = self.base_url,
            params = self.params
            # {k:v for (k,v) in self.params.items() if v is not None},
            )
        
        # Check if the request was successful
        if response.status_code == 200:
            # Insert fetched data into self.data
            if response.json().get('features'):
                response_body = response.json().get('features') # If response body is a list of objects
            else:
                response_body = [response.json()] # If response body is a single object
            
            if response_body[0].get('properties'):
                self.data.extend([data.get('properties') for data in response_body])
            else:
                self.data.extend([data for data in response_body])
            # Extract headers from the first data entry
            if self.data and not self.headers:
                self.headers = [headers for headers in self.data[0].keys()]
            # Log the successful data retrieval
            logger.info(f"Data fetched successfully from {self.base_url}")
        else:
            logger.error(f"Error: {response.status_code}, {response.reason}")

    
    def save_to_csv(self, dir, filename, mode='w'):
        """
        Save the provided data to a CSV file.

        :param dir: The directory containing the output CSV file.
        :param filename: The name of the output CSV file.
        :param mode: The mode in which to open the file ('w' for write, 'a' for append).
        """
        # Check if data is available
        if self.data == []:
            logger.exception("No data to save.")
            return

        # Check if headers are available
        if self.headers == []:
            logger.exception("No headers available.")
            return
        
        # Construct path to the output file
        output_dir = os.path.join(AIRFLOW_HOME, dir)
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


class StationIngestor(BaseIngestor):
    def __init__(self, params: dict = None):
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
        self._fetch_data()

    def get_station_id(self) -> set:
        """
        Get all station IDs from the data.
        """
        if self.data:
            return {station.get('stationIdentifier', None) for station in self.data}
        else:
            logger.exception("No data available to extract station IDs.")
            return None
        
    def get_county_id(self) -> set:
        """
        Get all county IDs from the data.
        """
        if self.data:
            county_endpoints = {station.get('county', None) for station in self.data}
            county_ids = map(lambda x: x[-6:], county_endpoints)
            return county_ids
        else:
            logger.exception("No data available to extract county IDs.")
            return None


class CountyIngestor(BaseIngestor):
    def __init__(self, county_ids):
        super().__init__()
        self.county_ids = county_ids

        for county_id in self.county_ids:
            # Update the base URL with county endpoint
            self.endpoint = f"zones/county/{county_id}"
            self.base_url = "".join(["https://api.weather.gov/", self.endpoint])

            # Fetch data
            self._fetch_data()

    def get_office_id(self) -> set:
        """
        Get all office IDs from the data.
        """
        if self.data:
            office_endpoints = {county.get('forecastOffice', None) for county in self.data}
            office_ids = map(lambda x: x[-3:], office_endpoints)
            return office_ids
        else:
            logger.exception("No data available to extract office IDs.")
            return None


class OfficeIngestor(BaseIngestor):
    def __init__(self, office_ids):
        super().__init__()
        self.office_ids = office_ids

        for office_id in self.office_ids:
            # Update the base URL with county endpoint
            self.endpoint = f"offices/{office_id}"
            self.base_url = "".join(["https://api.weather.gov/", self.endpoint])

            # Fetch data
            self._fetch_data()


class ObservationIngestor(BaseIngestor):
    def __init__(self, station_ids, params: dict = None):
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
        self.station_ids = station_ids

        # Check if station_id is provided
        if not station_ids:
            logger.error("Station ID is required for observations ingestion.")
            raise ValueError("Station ID is required for observations ingestion.")

        # Check if station_id is a list or a single value
        # if isinstance(station_id, list):
        #     self.station_id = station_id
        # else:
        #     self.station_id = [station_id]
        
        for station_id in self.station_ids:
            # Update the base URL with the station ID
            self.endpoint = f"stations/{station_id}/observations"
            self.base_url = "".join(["https://api.weather.gov/", self.endpoint])
            
            # Fetch the observations data  
            self._fetch_data()
        
    
if __name__ == "__main__":
    # Getting stations data in California
    station_data = StationIngestor(
        params={
            "state": "CA",
            "limit": 2}
        )
    station_data.save_to_csv(dir="data/stations", filename="stations.csv")
    
    # Getting county data 
    county_data = CountyIngestor(
        county_ids=station_data.get_county_id()
    )
    county_data.save_to_csv(dir="data/counties", filename="counties.csv")

    # Getting office data
    office_data = OfficeIngestor(
        office_ids=county_data.get_office_id()
    )
    office_data.save_to_csv(dir="data/offices", filename="offices.csv")

    # Setting the start and end time for the observations
    start = (now - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(hour=23, minute=59, second=59, microsecond=0)
   
   # Getting 100 observations of each station on the previous day
    observation_data = ObservationIngestor(
        station_ids = station_data.get_station_id(), 
        params={
            "start": start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "end": end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "limit": 100}
        )
    observation_data.save_to_csv(dir="data/observations", filename=f"{start.strftime('%Y%m%d')}_observations.csv")
