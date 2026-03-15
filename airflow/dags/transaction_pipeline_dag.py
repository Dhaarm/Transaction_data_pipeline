"""
DAG: transaction_pipeline
Purpose: Orchestrate the end-to-end data pipeline:
1. Generate transaction data
2. Upload data to Delta Lake
3. Run Spark ETL and load results into ScyllaDB
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging


# -------------------------------------------------------
# Logging configuration
# -------------------------------------------------------
logger = logging.getLogger(__name__)


# -------------------------------------------------------
# Failure Callback Function
# This will run if any task fails
# -------------------------------------------------------
def task_failure_callback(context):
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    logger.error(
        f"Task Failed: {task_instance.task_id} "
        f"in DAG: {dag_run.dag_id} "
        f"Run ID: {dag_run.run_id}"
    )


# -------------------------------------------------------
# Default DAG Arguments
# These apply to all tasks unless overridden
# -------------------------------------------------------
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_callback
}


# -------------------------------------------------------
# Define DAG
# schedule_interval="@daily" runs pipeline once per day
# catchup=False prevents backfilling old runs
# -------------------------------------------------------
with DAG(
    dag_id="transaction_pipeline",
    description="Daily transaction processing pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["data-engineering", "spark", "delta"]
) as dag:


    # -------------------------------------------------------
    # Task 1: Generate Fake Transaction Data
    # -------------------------------------------------------
    generate_data = BashOperator(
        task_id="generate_data",

        # set -e ensures the task fails immediately on error
        bash_command="""
        set -e
        echo "Starting data generation..."
        python /opt/spark/scripts/generate_data.py
        echo "Data generation completed"
        """
    )


    # -------------------------------------------------------
    # Task 2: Upload CSV Data to Delta Lake
    # -------------------------------------------------------
    upload_delta = BashOperator(
        task_id="upload_to_delta",

        bash_command="""
        set -e
        echo "Uploading data to Delta Lake..."

        spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-spark_2.12:3.0.0 \
        /opt/spark/scripts/upload_to_delta.py

        echo "Upload to Delta completed"
        """
    )


    # -------------------------------------------------------
    # Task 3: Run Spark ETL Job
    # Performs cleaning, aggregation, and loads into ScyllaDB
    # -------------------------------------------------------
    spark_etl = BashOperator(
        task_id="spark_etl",

        bash_command="""
        set -e
        echo "Starting Spark ETL job..."

        spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-spark_2.12:3.0.0 \
        /opt/spark/scripts/spark_etl.py

        echo "Spark ETL completed successfully"
        """
    )


    # -------------------------------------------------------
    # Task Dependencies
    # Ensures correct execution order
    # -------------------------------------------------------
    generate_data >> upload_delta >> spark_etl