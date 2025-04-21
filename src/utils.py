
import os
import csv

def save_to_csv(data, header, filename):
    """
    Save the provided data to a CSV file.
    :param filename: The name of the output CSV file.
    """
    # Navigate to the output folder
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "../data")
    # Create the output folder if it doesn't exist
    os.makedirs(output_dir, exist_ok=True) 
    # Construct the output file path
    file_path = os.path.join(output_dir, filename)
    
    # Write to csv
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # writer.writerow(['ingestionTime','stationId', 'stationName', 'cityName', 'forecastEndpoint', 'countyEndpoint'])
        writer.writerows(header)
        writer.writerows(data)
    print(f"Data saved to {file_path}")