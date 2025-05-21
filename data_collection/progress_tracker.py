"""
Progress tracking utilities for data collection
"""
from pathlib import Path

class ProgressTracker:
    """Tracks completed API requests to avoid redundant data collection."""
    
    def __init__(self, log_file_path):
        """
        Initialize the progress tracker.
        
        Args:
            log_file_path: Path to the file tracking completed sessions
        """
        self.log_file_path = Path(log_file_path)
        self.completed_items = set()
        self._load_completed()
        
    def _load_completed(self):
        """Load the set of completed items from the log file."""
        if self.log_file_path.exists():
            with open(self.log_file_path, 'r') as f:
                self.completed_items = set(f.read().splitlines())
        else:
            # Create directory if it doesn't exist
            self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty file
            with open(self.log_file_path, 'w') as f:
                pass
    
    def is_completed(self, item_key):
        """
        Check if an item has already been processed.
        
        Args:
            item_key: Unique identifier for the data item
            
        Returns:
            bool: True if the item has been completed
        """
        return item_key in self.completed_items
    
    def mark_completed(self, item_key):
        """
        Mark an item as completed and save to the log file.
        
        Args:
            item_key: Unique identifier for the data item
        """
        if item_key not in self.completed_items:
            with open(self.log_file_path, 'a') as f:
                f.write(f"{item_key}\n")
            self.completed_items.add(item_key)
    
    def get_all_completed(self):
        """
        Return the set of all completed items.
        
        Returns:
            set: Set of completed item keys
        """
        return self.completed_items