import logging
import os
# import dotenv
from datetime import datetime


# Env variables
# dotenv.load_dotenv()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/db")
now = datetime.now()

def setup_logger(logs_filename: str):
    """
    Set up logging configuration.
    
    :param log_filename: Name of the log file.
    """
    logs_dir = os.path.join(AIRFLOW_HOME, "logs", "scripts")

    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)

    logs_filepath = os.path.join(logs_dir, logs_filename)

    logging.basicConfig(
        filename=logs_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    return logger

