import pendulum
import os
from datetime import timedelta
from airflow.sdk import dag, task
from scripts.ingestion import StationIngestor, CountyIngestor, OfficeIngestor, ObservationIngestor
from scripts.utils import now

ingestion_state         = os.environ.get("INGESTION_STATE", "CA").split(",")
ingestion_station_limit = os.environ.get("INGESTION_STATION_LIMIT", 10)
ingestion_obs_limit     = os.environ.get("INGESTION_OBS_LIMIT", 100)

# Pythonic airflow dag
@dag(
    schedule=None,
    start_date=pendulum.datetime(year=2025, month=6, day=22, tz="UTC"),
    catchup=False,
    tags=["ingestion"]
) 
def ingestion_dag():
    @task(multiple_outputs=True)
    def ingest_station_data():
        station_ingestor = StationIngestor(
            params={"state": ingestion_state, "limit": ingestion_station_limit}
            )
        station_ingestor._save_to_csv(dir="data/stations", filename="stations.csv")
        return {
            # Convert to list for XCom serialization rule
            "station_ids": list(station_ingestor._get_station_id()), 
            "county_ids": list(station_ingestor._get_county_id())
            }
    
    @task()
    def ingest_county_data(xcom_county_ids):
        county_ingestor = CountyIngestor(
            # station_outputs is an XcomArg obj, can't use station_outputs["county_ids"] directly here
            county_ids=xcom_county_ids
            )
        county_ingestor._save_to_csv(dir="data/counties", filename="counties.csv")
        return list(county_ingestor._get_office_id())
    
    @task()
    def ingest_office_data(xcom_office_ids):
        office_ingestor = OfficeIngestor(
            office_ids=xcom_office_ids
            )
        office_ingestor._save_to_csv(dir="data/offices", filename="offices.csv")

    @task
    def ingest_observation_data(xcom_station_ids):
        # Setting the start and end time for the observations
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(hour=23, minute=59, second=59, microsecond=0)
        observation_ingestor = ObservationIngestor(
            station_ids=xcom_station_ids,
            params={
                "start": start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end": end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "limit": ingestion_obs_limit
                }
            )
        observation_ingestor._save_to_csv(dir="data/observations", filename=f"{start.strftime('%Y%m%d')}_observations.csv")
    
    station_outputs = ingest_station_data()
    observation_output = ingest_observation_data(xcom_station_ids=station_outputs["station_ids"])
    county_output = ingest_county_data(xcom_county_ids=station_outputs["county_ids"])
    office_output = ingest_office_data(xcom_office_ids=county_output)
    
ingestion_dag()