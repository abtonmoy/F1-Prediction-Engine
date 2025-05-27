"""
FastF1 Data Collector
---------------------
Collects historical F1 data using FastF1 API with rate limiting and progress tracking.
"""
import fastf1
import time
import logging
from fastf1.req import RateLimitExceededError
from pathlib import Path
import sys
sys.path.append('..')  # parent directory

from config.settings import CACHE_DIR, LOG_DIR, BACKOFF_TIME
from utils.rate_limiter import apply_rate_limiting
from data_collection.progress_tracker import ProgressTracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(LOG_DIR) / 'fastf1_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FastF1Collector:
    """Handles collection of F1 data via FastF1 API with progress tracking."""
    
    def __init__(self, years=range(2017, 2025), sessions=None):
        """
        Initialize the FastF1 collector.
        
        Args:
            years: Range of years to collect data for
            sessions: List of session types to collect (defaults to all main sessions)
        """
        self.years = list(years)
        self.sessions = sessions or ['FP1', 'FP2', 'FP3', 'Q', 'R']
        
        # Initialize and enable cache
        self.cache_dir = Path(CACHE_DIR)
        self.cache_dir.mkdir(exist_ok=True)
        fastf1.Cache.enable_cache(str(self.cache_dir))
        
        # Initialize progress tracker
        self.progress_tracker = ProgressTracker(LOG_DIR / 'completed_sessions.txt')
        
    def session_key(self, year, rnd, sess):
        """Generate a unique key for a session."""
        return f"{year}_{rnd}_{sess}"
    
    @apply_rate_limiting
    def fetch_session(self, year, rnd, sess):
        """Fetch a single session with rate limiting applied."""
        try:
            race = fastf1.get_session(year, rnd, sess)
            race.load()
            return race
        except RateLimitExceededError:
            logger.warning("Rate limit exceeded. Backing off...")
            time.sleep(BACKOFF_TIME)  # Default backoff time from settings
            raise  # Re-raise to let the rate limiter handle it
        except Exception as e:
            logger.error(f"Failed to load session for {year} R{rnd} {sess}: {e}")
            raise
    
    def collect_all(self):
        """Collect all F1 data for configured years and sessions."""
        for year in self.years:
            self.collect_year(year)
    
    def collect_year(self, year):
        """Collect all F1 data for a specific year."""
        logger.info(f"Collecting data for {year} season")
        
        try:
            schedule = fastf1.get_event_schedule(year, include_testing=False)
            
            for _, event in schedule.iterrows():
                rnd = event['RoundNumber']
                event_name = event['EventName']
                
                logger.info(f"Processing {year} R{rnd}: {event_name}")
                
                for sess in self.sessions:
                    self.collect_session(year, rnd, sess)
                    
        except Exception as e:
            logger.error(f"Error collecting data for {year}: {e}")
    
    def collect_session(self, year, rnd, sess):
        """Collect data for a specific session."""
        key = self.session_key(year, rnd, sess)
        
        if self.progress_tracker.is_completed(key):
            logger.info(f"✓ Already collected: {year} R{rnd} {sess}")
            return
        
        logger.info(f"Collecting: {year} R{rnd} {sess}")
        
        try:
            session_data = self.fetch_session(year, rnd, sess)
            self.progress_tracker.mark_completed(key)
            logger.info(f"✓ Successfully collected: {year} R{rnd} {sess}")
            return session_data
            
        except Exception as e:
            logger.error(f"Failed to collect {year} R{rnd} {sess}: {e}")
            return None

