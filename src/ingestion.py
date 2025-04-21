"""
Crawl data from NOAA API endpoint
""" 

import requests
import os
import csv
import json
import datetime

class Ingestion:
    def __init__(self, state:str=None, limit:int=None):
        """
        Initialize the base class for weather data ingestion.
        :param state: The state code for the weather data (optional).
        :param limit: The maximum number of records to retrieve (optional).
        """
        self.state = state
        self.limit = limit
        self.base_url = "https://api.weather.gov/"
        self.endpoint = None
        self.headers = None
        self.data = None

        # Send GET request to the API
        self.fetch_data()
    

    def fetch_data(self):
        """
        Get the data from the NOAA API.
        """
        response = requests.get(
            self.base_url,
            params={
                "state":self.state if self.state else None,
                "limit":self.limit if self.limit else None
                }
            )
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response into data and headers
            self.data = [data.get('properties',[]) for data in response.json().get('features', [])]
            if self.data:
                self.headers = [headers for headers in self.data[0].keys()]
        else:
            print(f"Error: {response.status_code}")
        
    
    def save_to_csv(self, filename):
        """
        Save the provided data to a CSV file.
        :param filename: The name of the output CSV file.
        """
        # Check if data is available
        if not self.data:
            print("No data to save.")
            return

        # Check if headers are available
        if not self.headers:
            print("No headers available.")
            return
        
        # Construct path to the output file
        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, "../data")
        os.makedirs(output_dir, exist_ok=True) 
        file_path = os.path.join(output_dir, filename)
        
        # Write to csv
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write the headers
            writer.writerow(self.headers)
            # Write the data
            for row in self.data:
                # Extract the values from the row
                values = [row.get(header, '') for header in self.headers]
                writer.writerow(values)
        print(f"Data saved to {file_path}")

class StationsIngestion(Ingestion):
    def __init__(self, state:str=None, limit:int=None):
        """
        Initialize the StationData class for weather station data ingestion.
        :param state: The state code for the weather data (optional).
        :param limit: The maximum number of records to retrieve (optional).
        """
        # Call the base class constructor
        super().__init__(state, limit)

        # Update the base URL with stations endpoint
        self.endpoint = "stations"
        self.base_url = "".join(["https://api.weather.gov/", self.endpoint])
        
        # Fetch the stations data  
        self.fetch_data()
        

def main():
    # Example usage
    station_data = StationsIngestion(state="CA", limit=10)
    station_data.save_to_csv("stations.csv")
    
if __name__ == "__main__":
    main()
