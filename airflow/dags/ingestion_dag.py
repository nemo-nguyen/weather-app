import pendulum
from datetime import datetime, timedelta
# from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from scripts.ingestion import StationIngestor, CountyIngestor, OfficeIngestor, ObservationIngestor
from scripts.utils import now

# Pythonic airflow dag
@dag(
    schedule=None,
    start_date=pendulum.datetime(year=2025, month=6, day=22, tz="UTC"),
    catchup=False
) 
def ingestion_dag():
    @task(multiple_outputs=True)
    def ingest_station_data():
        station_ingestor = StationIngestor(
            params={"state":"CA", "limit":10}
            )
        station_ingestor._save_to_csv(dir="data/stations", filename="stations.csv")
        return {
            "station_ids": station_ingestor._get_station_id(), 
            "county_ids": station_ingestor._get_county_id()
            }
    station_outputs = ingest_station_data()

    @task()
    def ingest_county_data():
        county_ingestor = CountyIngestor(
            county_ids=station_outputs["county_ids"]
            )
        county_ingestor._save_to_csv(dir="data/counties", filename="counties.csv")
        office_ids = county_ingestor._get_office_id()
        return office_ids
    county_output = ingest_county_data()

    @task()
    def ingest_office_data():
        office_ingestor = OfficeIngestor(
            office_ids=county_output
            )
        office_ingestor._save_to_csv(dir="data/offices", filename="offices.csv")
    ingest_office_data()

    @task
    def ingest_observation_data():
        # Setting the start and end time for the observations
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(hour=23, minute=59, second=59, microsecond=0)
        observation_ingestor = ObservationIngestor(
            station_ids=station_outputs["station_ids"],
            params={
                "start": start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end": end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "limit": 100
            }
            )
        observation_ingestor._save_to_csv(dir="data/observations", filename=f"{start.strftime('%Y%m%d')}_observations.csv")
    ingest_observation_data()

ingestion_dag()