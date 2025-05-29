"""
Settings for the F1 data pipeline
"""
from pathlib import Path
import os

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = os.environ.get('CACHE_DIR', BASE_DIR / 'cache')
LOG_DIR = os.environ.get('LOG_DIR', BASE_DIR / 'logs')
DATA_DIR = os.environ.get('DATA_DIR', BASE_DIR / 'data')

# Create directories if they don't exist
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

# API Settings
OPENF1_BASE_URL = "https://api.openf1.org/v1"

# Rate limiting settings
BACKOFF_TIME = 3600  # Default backoff time in seconds when rate limit is exceeded

# Database settings
DB_CONFIG = {
    'engine': os.environ.get('DB_ENGINE', 'postgresql'),
    'username': os.environ.get('DB_USERNAME', 'f1user'),
    'password': os.environ.get('DB_PASSWORD', 'f1password'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'f1data'),
}

# Data collection settings
DEFAULT_YEARS = range(2017, 2025)
DEFAULT_SESSIONS = ['FP1', 'FP2', 'FP3', 'Q', 'R']

# Reconciliation settings
RECONCILIATION_PRIORITY = {
    'live_timing': 'openf1',  # OpenF1 is better for live timing data
    'telemetry': 'fastf1',    # FastF1 is better for detailed telemetry
    'default': 'fastf1'       # Default to FastF1 for anything else
}

# Airflow settings
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', BASE_DIR / 'airflow')
AIRFLOW_DAG_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')

# Schedule settings
HISTORICAL_SCHEDULE = "0 0 * * *"  # Daily at midnight
LIVE_DATA_SCHEDULE = "*/5 * * * *"  # Every 5 minutes during race weekends