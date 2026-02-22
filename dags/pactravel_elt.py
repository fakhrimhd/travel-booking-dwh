"""
Pactravel ELT Pipeline DAG

Orchestrates the complete ELT workflow:
1. Extract data from source PostgreSQL
2. Load data to data warehouse
3. Transform with dbt (staging → marts)
4. Run dbt tests for data quality

Author: Fakhri Muhammad
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import extract and load functions
import sys
sys.path.insert(0, '/opt/airflow')

from extract import extract
from load import load


# Default arguments for all tasks
default_args = {
    'owner': 'fakhri',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='pactravel_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for Pactravel travel booking analytics',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pactravel', 'elt', 'dbt'],
    doc_md="""
    ## Pactravel ELT Pipeline
    
    This DAG orchestrates the complete ELT workflow for the Pactravel 
    travel booking data warehouse.
    
    ### Tasks
    1. **extract** - Pull data from source PostgreSQL to CSV
    2. **load** - Load CSVs to data warehouse staging
    3. **dbt_run** - Execute dbt models (staging → marts)
    4. **dbt_test** - Run data quality tests
    
    ### Data Flow
    ```
    Source DB → Extract → CSV → Load → DWH → dbt → Star Schema
    ```
    """,
) as dag:

    # Task 1: Extract data from source
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        doc_md="Extract data from source PostgreSQL database to CSV files.",
    )

    # Task 2: Load data to warehouse
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        doc_md="Load CSV files to data warehouse staging tables.",
    )

    # Task 3: Run dbt models (via docker exec to dbt-runner container)
    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='docker exec dbt-runner dbt run --profiles-dir .',
        doc_md="Execute dbt models to transform staging data into star schema.",
    )

    # Task 4: Run dbt tests (via docker exec to dbt-runner container)
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='docker exec dbt-runner dbt test --profiles-dir .',
        doc_md="Run dbt tests to validate data quality and referential integrity.",
    )

    # Define task dependencies
    extract_task >> load_task >> dbt_run_task >> dbt_test_task
