"""
Data Reconciliation Engine
--------------------------
Reconciles data from multiple sources (FastF1 and OpenF1) into a unified dataset.
"""
import pandas as pd
import numpy as np
import logging
from pathlib import Path
import sys

sys.path.append('..')  # Add parent directory to path

from config.settings import LOG_DIR, RECONCILIATION_PRIORITY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(LOG_DIR) / 'reconciliation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReconciliationEngine:
    """
    Engine for reconciling data from multiple F1 data sources.
    """
    
    def __init__(self):
        """Initialize the reconciliation engine."""
        self.priority_map = RECONCILIATION_PRIORITY
    
    def reconcile_session_data(self, fastf1_data, openf1_data, data_type):
        """
        Reconcile data between FastF1 and OpenF1 sources.
        
        Args:
            fastf1_data: Data from FastF1
            openf1_data: Data from OpenF1
            data_type: Type of data being reconciled (timing, telemetry, etc.)
            
        Returns:
            pd.DataFrame: Reconciled dataset
        """
        if fastf1_data is None and openf1_data is None:
            logger.warning("No data available from either source")
            return None
        
        if fastf1_data is None:
            logger.info("Using OpenF1 data only (FastF1 data unavailable)")
            return openf1_data
        
        if openf1_data is None:
            logger.info("Using FastF1 data only (OpenF1 data unavailable)")
            return fastf1_data
        
        # Determine priority source based on data type
        priority_source = self.priority_map.get(data_type, self.priority_map['default'])
        logger.info(f"Reconciling {data_type} data with priority source: {priority_source}")
        
        # Perform reconciliation based on data type
        if data_type == 'timing':
            return self._reconcile_timing_data(fastf1_data, openf1_data, priority_source)
        elif data_type == 'telemetry':
            return self._reconcile_telemetry_data(fastf1_data, openf1_data, priority_source)
        else:
            # Generic reconciliation for other data types
            return self._generic_reconciliation(fastf1_data, openf1_data, priority_source)
    
    def _reconcile_timing_data(self, fastf1_data, openf1_data, priority_source):
        """Reconcile timing data from both sources."""
        try:
            # Ensure both dataframes have compatible index
            if 'Time' not in fastf1_data.columns and 'time' in fastf1_data.columns:
                fastf1_data = fastf1_data.rename(columns={'time': 'Time'})
            
            if 'Time' not in openf1_data.columns and 'time' in openf1_data.columns:
                openf1_data = openf1_data.rename(columns={'time': 'Time'})
            
            # Create a common driver ID column if needed
            if 'DriverNumber' in fastf1_data.columns and 'driver_number' in openf1_data.columns:
                fastf1_data['driver_number'] = fastf1_data['DriverNumber']
                
            # Merge on a common key (usually time and driver)
            merge_cols = [col for col in ['Time', 'driver_number'] if col in fastf1_data.columns and col in openf1_data.columns]
            
            if not merge_cols:
                logger.warning("No common columns found for merging timing data")
                return fastf1_data if priority_source == 'fastf1' else openf1_data
            
            merged = pd.merge(
                fastf1_data, 
                openf1_data,
                on=merge_cols,
                how='outer',
                suffixes=('_fastf1', '_openf1')
            )
            
            # For each column that appears in both sources, choose based on priority
            for col in merged.columns:
                if col.endswith('_fastf1') and col.replace('_fastf1', '_openf1') in merged.columns:
                    base_col = col.replace('_fastf1', '')
                    
                    if priority_source == 'fastf1':
                        merged[base_col] = merged[col].combine_first(merged[f"{base_col}_openf1"])
                    else:
                        merged[base_col] = merged[f"{base_col}_openf1"].combine_first(merged[col])
                    
                    # Drop the suffix columns
                    merged = merged.drop(columns=[col, f"{base_col}_openf1"])
            
            return merged
            
        except Exception as e:
            logger.error(f"Error reconciling timing data: {e}")
            # Fall back to priority source
            return fastf1_data if priority_source == 'fastf1' else openf1_data
    
    def _reconcile_telemetry_data(self, fastf1_data, openf1_data, priority_source):
        """Reconcile telemetry data from both sources."""
        try:
            # FastF1 usually has better telemetry, so default to using it
            # But we can fill gaps with OpenF1 data where possible
            
            # First, standardize column names
            fastf1_mapping = {
                'Speed': 'speed',
                'RPM': 'rpm',
                'Throttle': 'throttle',
                'Brake': 'brake',
                'DRS': 'drs',
                'Time': 'time'
            }
            
            openf1_mapping = {
                'speed': 'speed',
                'rpm': 'rpm',
                'throttle': 'throttle',
                'brake': 'brake',
                'drs': 'drs',
                'time': 'time'
            }
            
            # Rename columns to standard names
            fastf1_renamed = fastf1_data.rename(columns={k: v for k, v in fastf1_mapping.items() if k in fastf1_data.columns})
            openf1_renamed = openf1_data.rename(columns={k: v for k, v in openf1_mapping.items() if k in openf1_data.columns})
            
            # Determine the common time range
            min_time = max(fastf1_renamed['time'].min(), openf1_renamed['time'].min())
            max_time = min(fastf1_renamed['time'].max(), openf1_renamed['time'].max())
            
            # Filter to common time range
            fastf1_filtered = fastf1_renamed[(fastf1_renamed['time'] >= min_time) & (fastf1_renamed['time'] <= max_time)]
            openf1_filtered = openf1_renamed[(openf1_renamed['time'] >= min_time) & (openf1_renamed['time'] <= max_time)]
            
            # Choose priority source as base
            if priority_source == 'fastf1':
                base_data = fastf1_filtered
                secondary_data = openf1_filtered
            else:
                base_data = openf1_filtered
                secondary_data = fastf1_filtered
            
            # Merge on a common key (usually time and driver)
            merged = pd.merge_asof(
                base_data.sort_values('time'),
                secondary_data.sort_values('time'),
                on='time',
                tolerance=pd.Timedelta('0.1s'),
                direction='nearest',
                suffixes=('', '_secondary')
            )
            
            # Fill missing data from secondary source where applicable
            for col in [c for c in secondary_data.columns if c != 'time']:
                if col in base_data.columns and f"{col}_secondary" in merged.columns:
                    merged[col] = merged[col].combine_first(merged[f"{col}_secondary"])
                    merged = merged.drop(columns=[f"{col}_secondary"])
            
            return merged
            
        except Exception as e:
            logger.error(f"Error reconciling telemetry data: {e}")
            # Fall back to priority source
            return fastf1_data if priority_source == 'fastf1' else openf1_data
    
    def _generic_reconciliation(self, fastf1_data, openf1_data, priority_source):
        """Generic reconciliation for other data types."""
        try:
            # For generic data, use the priority source as the base
            # and fill in missing values from the secondary source
            primary_df = fastf1_data if priority_source == 'fastf1' else openf1_data
            secondary_df = openf1_data if priority_source == 'fastf1' else fastf1_data
            
            # If the dataframes have different structures, just use the primary
            if primary_df.shape[1] != secondary_df.shape[1] or not all(primary_df.columns == secondary_df.columns):
                logger.warning("Data structures differ between sources, using priority source only")
                return primary_df
            
            # Otherwise, use primary but fill NaN values from secondary
            result = primary_df.copy()
            for col in result.columns:
                result[col] = result[col].combine_first(secondary_df[col])
                
            return result
            
        except Exception as e:
            logger.error(f"Error in generic reconciliation: {e}")
            # Fall back to priority source
            return fastf1_data if priority_source == 'fastf1' else openf1_data
    
    def reconcile_full_session(self, fastf1_session, openf1_session):
        """
        Reconcile all data types for a complete session.
        
        Args:
            fastf1_session: Complete session data from FastF1
            openf1_session: Complete session data from OpenF1
            
        Returns:
            dict: Dictionary with reconciled data for each data type
        """
        reconciled_data = {}
        
        # Define common data types between sources
        data_types = ['timing', 'telemetry', 'weather', 'car_data', 'position']
        
        for data_type in data_types:
            fastf1_data = fastf1_session.get(data_type) if fastf1_session else None
            openf1_data = openf1_session.get(data_type) if openf1_session else None
            
            reconciled = self.reconcile_session_data(fastf1_data, openf1_data, data_type)
            if reconciled is not None:
                reconciled_data[data_type] = reconciled
        
        return reconciled_data