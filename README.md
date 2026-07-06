import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters passed in from Airflow / Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---- Read raw IEEE-CIS transaction data from S3 ----
input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

df = spark.read.csv(input_path, header=True, inferSchema=True)

print(f"Row count before cleaning: {df.count()}")

# ---- Basic cleaning / transformation ----
# Drop rows with null transaction amount
df_clean = df.dropna(subset=["TransactionAmt"])

# Flag high-value transactions (example fraud-relevant feature)
df_clean = df_clean.withColumn(
    "is_high_value",
    (df_clean["TransactionAmt"] > 500).cast("int")
)

print(f"Row count after cleaning: {df_clean.count()}")

# ---- Write processed output back to S3 ----

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "fatema",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finflow_batch_glue_pipeline",
    default_args=default_args,
    description="Triggers AWS Glue ETL job for FinFlow batch layer",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finflow", "glue", "batch"],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id="run_finflow_glue_etl",
        job_name="finflow-etl-job", # <-- must match your AWS Glue job name
        script_location="s3://your-bucket/scripts/etl_job.py", # <-- update bucket
        aws_conn_id="aws_default",
        region_name="us-east-1", # <-- update to your region
        script_args={
            "--INPUT_PATH": "s3://your-bucket/raw/ieee-cis/",
            "--OUTPUT_PATH": "s3://your-bucket/processed/ieee-cis/",
        },
    )

    wait_for_glue_job = GlueJobSensor(
        task_id="wait_for_finflow_glue_etl",
        job_name="finflow-etl-job",
        run_id="{{ task_instance.xcom_pull(task_ids='run_finflow_glue_etl') }}",
        aws_conn_id="aws_default",
    )

    run_glue_job >> wait_for_glue_job

    local airflow environment
    version: "3.8"

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.9.0
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ""
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_DEFAULT_REGION: us-east-1
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  depends_on:
    - postgres

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - "airflow db init && airflow users create --username admin --password admin --firstname Fatema --lastname RE --role Admin --email admin@example.com"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

volumes:
  postgres-db-volume:

  Quick start
  cd airflow
docker-compose up airflow-init
docker-compose up -d
df_clean.write.mode("overwrite").parquet(output_path)

job.commit()
