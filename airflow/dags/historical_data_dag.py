"""
Historical Data Collection DAG for Airflow
-----------------------------------------
Collects historical F1 data using FastF1 API.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.settings import DEFAULT_YEARS, DEFAULT_SESSIONS, HISTORICAL_SCHEDULE
from data_collection.fastf1_collector import FastF1Collector
from data_transformation.transform_fastf1_data import transform_fastf1_data
from database.insert_raw import insert_fastf1_session
from database.insert_transformed import insert_transformed_session

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'queue': 'default',
    'pool': 'default_pool',
}

# Create DAG
dag = DAG(
    'f1_historical_data_collection',
    default_args=default_args,
    description='Collect historical F1 data using FastF1 API',
    schedule_interval=HISTORICAL_SCHEDULE,
    start_date=days_ago(1),
    catchup=False,
    tags=['f1', 'historical', 'fastf1'],
)

def collect_historical_session(year, round_num, session_type, **kwargs):
    """
    Task to collect data for a specific historical session.
    
    Args:
        year: Year of the session
        round_num: Round number of the session
        session_type: Type of session (FP1, FP2, FP3, Q, R)
    """
    collector = FastF1Collector()
    session_data = collector.collect_session(year, round_num, session_type)
    return session_data is not None

def transform_session_data(year, round_num, session_type, **kwargs):
    """
    Task to transform the collected session data.
    
    Args:
        year: Year of the session
        round_num: Round number of the session
        session_type: Type of session (FP1, FP2, FP3, Q, R)
    """
    ti = kwargs['ti']
    session_collected = ti.xcom_pull(task_ids=f'collect_session_{year}_{round_num}_{session_type}')
    
    if not session_collected:
        return False
    
    transformed_data = transform_fastf1_data(year, round_num, session_type)
    return transformed_data is not None

def store_raw_session(year, round_num, session_type, **kwargs):
    """
    Task to store the raw session data in the database.
    
    Args:
        year: Year of the session
        round_num: Round number of the session
        session_type: Type of session (FP1, FP2, FP3, Q, R)
    """
    ti = kwargs['ti']
    session_collected = ti.xcom_pull(task_ids=f'collect_session_{year}_{round_num}_{session_type}')
    
    if not session_collected:
        return False
    
    success = insert_fastf1_session(year, round_num, session_type)
    return success

def store_transformed_session(year, round_num, session_type, **kwargs):
    """
    Task to store the transformed session data in the database.
    
    Args:
        year: Year of the session
        round_num: Round number of the session
        session_type: Type of session (FP1, FP2, FP3, Q, R)
    """
    ti = kwargs['ti'] 
    transform_success = ti.xcom_pull(task_ids=f'transform_session_{year}_{round_num}_{session_type}')
    
    if not transform_success:
        return False
    
    success = insert_transformed_session(year, round_num, session_type)
    return success

def create_session_tasks(year, round_num, session_type):
    """
    Create a set of tasks for processing a specific session.
    
    Args:
        year: Year of the session
        round_num: Round number of the session
        session_type: Type of session (FP1, FP2, FP3, Q, R)
        
    Returns:
        tuple: (collect_task, transform_task, store_raw_task, store_transformed_task)
    """
    # Collection task
    collect_task = PythonOperator(
        task_id=f'collect_session_{year}_{round_num}_{session_type}',
        python_callable=collect_historical_session,
        op_kwargs={'year': year, 'round_num': round_num, 'session_type': session_type},
        dag=dag,
    )
    
    # Transform task
    transform_task = PythonOperator(
        task_id=f'transform_session_{year}_{round_num}_{session_type}',
        python_callable=transform_session_data,
        op_kwargs={'year': year, 'round_num': round_num, 'session_type': session_type},
        dag=dag,
    )
    
    # Store raw task
    store_raw_task = PythonOperator(
        task_id=f'store_raw_session_{year}_{round_num}_{session_type}',
        python_callable=store_raw_session,
        op_kwargs={'year': year, 'round_num': round_num, 'session_type': session_type},
        dag=dag,
    )
    
    # Store transformed task
    store_transformed_task = PythonOperator(
        task_id=f'store_transformed_session_{year}_{round_num}_{session_type}',
        python_callable=store_transformed_session,
        op_kwargs={'year': year, 'round_num': round_num, 'session_type': session_type},
        dag=dag,
    )
    
    # Define task dependencies
    collect_task >> [transform_task, store_raw_task]
    transform_task >> store_transformed_task
    
    return (collect_task, transform_task, store_raw_task, store_transformed_task)

# Create dynamic tasks for each year and session in a smart way
# Instead of processing all years at once, we'll focus on one year at a time
# to avoid rate limiting issues

# Get next year to process
def get_next_year_to_process():
    """
    Determine the next year to process based on what's already been done.
    This function would look at the database or tracking file to find which year 
    needs processing next.
    
    Returns:
        int: The next year to process
    """
    # This is a placeholder implementation
    # In a real system, you would check what's already in the database
    # For now, we'll just return the most recent year
    return max(DEFAULT_YEARS)

next_year = get_next_year_to_process()

# Create tasks for selected year
def create_year_tasks(year):
    """Create tasks for all sessions in a given year."""
    # Get the schedule for this year
    collector = FastF1Collector()
    
    # This is a placeholder for the actual implementation
    # In reality, you'd query the FastF1 API for the schedule
    schedule = [(1, 'Bahrain'), (2, 'Saudi Arabia'), (3, 'Australia')]  # Example
    
    tasks = []
    for round_num, _ in schedule:
        for session_type in DEFAULT_SESSIONS:
            session_tasks = create_session_tasks(year, round_num, session_type)
            tasks.extend(session_tasks)
    
    return tasks

# Generate tasks for the selected year
year_tasks = create_year_tasks(next_year)

# Add a final task to mark the year as complete
def mark_year_complete(year, **kwargs):
    """Mark a year as completely processed."""
    # This would update whatever tracking mechanism you're using
    print(f"Year {year} processing complete")
    return True

year_complete_task = PythonOperator(
    task_id=f'mark_year_{next_year}_complete',
    python_callable=mark_year_complete,
    op_kwargs={'year': next_year},
    dag=dag,
)

# Set dependencies so year_complete runs after all year tasks
for task in year_tasks:
    if task.task_id.startswith('store_transformed_session_'):
        task >> year_complete_task