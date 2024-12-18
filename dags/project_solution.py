import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/dags")

default_args = {
    "owner": "airflow",
    'start_date': datetime(2024, 12, 1, 0, 0),
}

dag = DAG(
    "Kostya_M_multi_hop_datalake",
    default_args=default_args,
    description="My multihop data pipeline",
    schedule_interval=None,
    tags=["KostyaM"],
)

# Start task landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id="Landing_to_Bronze",
    bash_command=f"python {BASE_PATH}/landing_to_bronze.py",
    dag=dag,
)

# Start task bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id="Bronze_to_Silver",
    bash_command=f"python {BASE_PATH}/bronze_to_silver.py",
    dag=dag,
)

# Start task silver_to_gold.py
silver_to_gold = BashOperator(
    task_id="Silver_to_Gold",
    bash_command=f"python {BASE_PATH}/silver_to_gold.py",
    dag=dag,
)

landing_to_bronze >> bronze_to_silver >> silver_to_gold
