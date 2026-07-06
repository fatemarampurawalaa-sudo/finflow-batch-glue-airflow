import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, col

# ---- Glue job boilerplate ----
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---- Config: update bucket name if yours differs ----
RAW_PATH = "s3://finflow-batch-fatem-2607/raw/creditcard.csv"
PROCESSED_PATH = "s3://finflow-batch-fatem-2607/processed/"

# ---- Step 1: Read raw CSV from S3 ----
df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)

print(f"Raw record count: {df.count()}")

# ---- Step 2: Basic cleaning ----
df_clean = df.dropDuplicates()
df_clean = df_clean.filter(col("Class").isNotNull())
df_clean = df_clean.withColumn("Class", col("Class").cast("int"))
df_clean = df_clean.withColumn("processed_at", current_timestamp())

print(f"Cleaned record count: {df_clean.count()}")

# ---- Step 3: Write output as Parquet, partitioned by Class ----
df_clean.write.mode("overwrite").partitionBy("Class").parquet(PROCESSED_PATH)

print("ETL job completed successfully.")

job.commit()
