import logging
import os
from datetime import datetime


now = datetime.now()
base_dir = os.path.dirname(os.path.abspath(__file__))

def setup_logger(logs_filename: str):
    """
    Set up logging configuration.
    
    :param log_filename: Name of the log file.
    """
    logs_dir = os.path.join(base_dir, "../logs")

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