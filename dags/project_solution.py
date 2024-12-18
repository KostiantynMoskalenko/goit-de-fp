import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/dags")

'''
DAGS_PATH = '/opt/airflow/dags'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1, 0, 0),
}

with DAG(
        'project_solution',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["KostyaM"],
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=f'{DAGS_PATH}/landing_to_bronze.py',
        name='landing_to_bronze',
        conn_id='spark_default',
    )
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f'{DAGS_PATH}/bronze_to_silver.py',
        name='bronze_to_silver',
        conn_id='spark_default',
    )
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f'{DAGS_PATH}/silver_to_gold.py',
        name='silver_to_gold',
        conn_id='spark_default',
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold

'''
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
