from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

with DAG(
    dag_id="olist_cdc_dbt",
    description="Run dbt staging model + SCD Type 2 snapshot for the olist CDC-lite pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist_project", "dbt","duckdb"]
) as dag:
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
            
        ),
        
    )   
    
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )
    
    
    dbt_run >> dbt_snapshot