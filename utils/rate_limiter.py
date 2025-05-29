"""
Rate limiting utility for API requests
"""
import time
import logging
import random
from functools import wraps

logger = logging.getLogger(__name__)

def apply_rate_limiting(func):
    """
    Decorator that applies rate limiting to API calls with exponential backoff.
    
    Args:
        func: The function to apply rate limiting to
        
    Returns:
        Function wrapper with rate limiting logic
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 5
        base_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Check if it's a rate limit error
                if "Rate limit" in str(e):
                    if attempt < max_retries - 1:  # Don't sleep on the last attempt
                        # Calculate delay with exponential backoff and jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        
                        # If it's the second attempt or later, use longer delays
                        if attempt >= 1:
                            delay = max(delay, 60)  # Minimum 1 minute
                        if attempt >= 2:
                            delay = max(delay, 300)  # Minimum 5 minutes
                        if attempt >= 3:
                            delay = max(delay, 1800)  # Minimum 30 minutes
                        
                        logger.warning(f"Rate limit exceeded. Retrying in {delay:.1f}s (attempt {attempt+1}/{max_retries})")
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed after {max_retries} attempts due to rate limiting")
                        raise
                else:
                    # Not a rate limit error, re-raise
                    raise
    
    return wrapper

def throttle_requests(min_interval=1.0):
    """
    Decorator to ensure a minimum time interval between function calls.
    
    Args:
        min_interval: Minimum seconds between calls
        
    Returns:
        Function wrapper that throttles calls
    """
    last_called = [0.0]  # Mutable default to track between calls
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()
            elapsed = current_time - last_called[0]
            
            # If not enough time has passed, sleep
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            
            result = func(*args, **kwargs)
            last_called[0] = time.time()  # Update after execution
            
            return result
        return wrapper
    return decorator