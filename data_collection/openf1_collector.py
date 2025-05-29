"""
OpenF1 Data Collector
---------------------
Collects live F1 data using OpenF1 API with rate limiting and progress tracking.
"""
import requests
import time
import json
import logging
from pathlib import Path
import sys
import pandas as pd
from datetime import datetime, timedelta

sys.path.append('..')  # Add parent directory to path

from config.settings import LOG_DIR, OPENF1_BASE_URL
from utils.rate_limiter import apply_rate_limiting, throttle_requests
from data_collection.progress_tracker import ProgressTracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(LOG_DIR) / 'openf1_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OpenF1Collector:
    """Handles collection of F1 data via OpenF1 API with progress tracking."""
    
    def __init__(self):
        """Initialize the OpenF1 collector."""
        self.base_url = OPENF1_BASE_URL
        
        # Initialize progress tracker
        self.progress_tracker = ProgressTracker(LOG_DIR / 'openf1_completed.txt')
        
    def session_key(self, session_date, session_type):
        """Generate a unique key for a session."""
        return f"{session_date}_{session_type}"
    
    @throttle_requests(min_interval=0.5)  # Throttle to max 2 requests per second
    @apply_rate_limiting
    def _make_api_request(self, endpoint, params=None):
        """Make a request to the OpenF1 API with rate limiting."""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Raise exception for non-2xx responses
            return response.json()
        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit exceeded. Retry after {retry_after}s")
                time.sleep(retry_after)
                raise Exception(f"Rate limit exceeded: {e}")
            else:
                logger.error(f"API request failed: {e}")
                raise
    
    def get_sessions(self, year=None):
        """
        Get a list of available sessions.
        
        Args:
            year: Optional year to filter sessions
            
        Returns:
            pd.DataFrame: DataFrame with session information
        """
        params = {}
        if year:
            params['year'] = year
            
        data = self._make_api_request('sessions', params)
        return pd.DataFrame(data)
    
    def get_live_session_data(self, session_key, data_type):
        """
        Get live data for the current or most recent session.
        
        Args:
            session_key: Unique identifier for the session
            data_type: Type of data to collect (e.g., 'position', 'timing', 'car_data')
            
        Returns:
            dict: The collected data
        """
        if self.progress_tracker.is_completed(f"{session_key}_{data_type}"):
            logger.info(f"✓ Already collected: {session_key} - {data_type}")
            return None
        
        logger.info(f"Collecting live data: {session_key} - {data_type}")
        
        try:
            data = self._make_api_request(data_type)
            
            # Mark as completed only if we got data
            if data:
                self.progress_tracker.mark_completed(f"{session_key}_{data_type}")
                logger.info(f"✓ Successfully collected: {session_key} - {data_type}")
            
            return data
        except Exception as e:
            logger.error(f"Failed to collect {session_key} - {data_type}: {e}")
            return None
    
    def collect_current_session(self):
        """
        Collect data for the currently ongoing session if any.
        
        Returns:
            dict: Dictionary containing all collected data types for the session
        """
        try:
            # Get the current session information
            sessions = self.get_sessions()
            
            if sessions.empty:
                logger.info("No active sessions found")
                return None
            
            # Sort by date and get the most recent session
            sessions['date'] = pd.to_datetime(sessions['date'])
            current_session = sessions.sort_values('date', ascending=False).iloc[0]
            
            session_date = current_session['date'].strftime('%Y-%m-%d')
            session_type = current_session['type']
            session_key = self.session_key(session_date, session_type)
            
            logger.info(f"Collecting data for session: {session_key}")
            
            # Collect different types of data
            data_types = ['position', 'timing', 'car_data', 'weather', 'driver_info']
            collected_data = {}
            
            for data_type in data_types:
                data = self.get_live_session_data(session_key, data_type)
                if data:
                    collected_data[data_type] = data
            
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting current session data: {e}")
            return None

# Example usage
if __name__ == "__main__":
    collector = OpenF1Collector()
    data = collector.collect_current_session()
    if data:
        print(f"Collected {len(data)} data types for current session")