from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

default_args = {
    "owner": "fatema",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finflow_batch_pipeline",
    default_args=default_args,
    description="Orchestrates the FinFlow Glue ETL job daily",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["finflow", "glue", "batch"],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id="run_finflow_glue_job",
        job_name="finflow-batch-etl",
        region_name="us-east-1",
        aws_conn_id="aws_default",
        wait_for_completion=False,
    )

    wait_for_glue_job = GlueJobSensor(
        task_id="wait_for_finflow_glue_job",
        job_name="finflow-batch-etl",
        run_id="{{ task_instance.xcom_pull(task_ids='run_finflow_glue_job') }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=1800,
    )

    run_glue_job >> wait_for_glue_job

