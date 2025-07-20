import pendulum
from airflow.sdk import dag, task
from scripts.processor import StationDataProcessor, CountyDataProcessor, OfficeDataProcessor, ObservationDataProcessor
from scripts.utils import now

# Pythonic airflow dag
@dag(
    schedule="30 23 * * */3", # Run every 3 days at 11:30PM 
    start_date=pendulum.datetime(year=2025, month=6, day=22, tz="UTC"),
    catchup=False,
    tags=["process"]
) 
def process_dag():
    @task()
    def process_station_data():
        station_processor = StationDataProcessor(dir="data/stations")
        station_processor.load_to_db(
            table_name="station",
            mode="o"
        )
    
    @task()
    def process_county_data():
        county_processor = CountyDataProcessor(dir="data/counties")
        county_processor.load_to_db(
            table_name="county",
            mode="o"
        )

    @task()
    def process_office_data():
        office_processor = OfficeDataProcessor(dir = "data/offices")
        office_processor.load_to_db(table_name="office", mode="o")

    @task()
    def process_observation_data():
        observation_processor = ObservationDataProcessor(dir="data/observations")
        observation_processor.load_to_db(table_name="observation", mode="a")

    process_stations    = process_station_data() 
    process_counties    = process_county_data()
    process_offices     = process_office_data()
    process_observations = process_observation_data()

    process_stations >> process_counties >> process_offices >> process_observations
    
process_dag()