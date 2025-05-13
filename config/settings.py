"""Configuration settings for the F1 Prediction Engine."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "f1_prediction_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Ergast API settings
ERGAST_API_BASE_URL = "http://ergast.com/api/f1"

# FastF1 cache directory
FAST_F1_CACHE_DIR = RAW_DATA_DIR / "fastf1_cache"
FAST_F1_CACHE_DIR.mkdir(exist_ok=True)

# F1 seasons to collect data for 
START_SEASON = 2000
END_SEASON = 2024
