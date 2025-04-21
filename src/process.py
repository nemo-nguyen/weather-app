# Process the data
records = []
# Loop through the features and extract relevant data
for station in stations['features']:
    record = (station['properties']['stationIdentifier'], 
        station['properties']['name'],
        station['properties']['timeZone'].split("/")[1],
        station['properties']['forecast'][-6:],
        station['properties']['county'][-6:]
        )
    records.append(record)
# Create a DataFrame from the records
stationDf = pd.DataFrame(records, 
                columns=['stationId', 'stationName', 'cityName', 'forecastId', 'countyId']
                )