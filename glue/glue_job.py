import sys

from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from awsglue.context import GlueContext

from awsglue.job import Job

from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ---------------------------------------------------
# Glue Context
# ---------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

print("Glue Job Started")

# ---------------------------------------------------
# Read MySQL Table
# ---------------------------------------------------

jdbc_url = "jdbc:mysql://Glue-Etl:3306/glue_demo"

connection_properties = {
    "user": "admin",
    "password": "YOUR_PASSWORD",
    "driver": "com.mysql.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="glue_demo",
    properties=connection_properties
)

print("MySQL Table Loaded")

df.show()

# ---------------------------------------------------
# Example Transformation
# ---------------------------------------------------

from pyspark.sql.functions import col

transformed_df = df.withColumn(
    "total_price",
    col("quantity") * col("price")
)

print("Transformation Completed")

transformed_df.show()

# ---------------------------------------------------
# Write Output to S3
# ---------------------------------------------------

output_path = "s3://glue-etl-mlops-pipeline/output/"

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("Output Written to S3")

# ---------------------------------------------------
# Commit Job
# ---------------------------------------------------

job.commit()

print("Glue Job Completed Successfully")
