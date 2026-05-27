import sys
import json
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "secret_name",
        "db_table",
        "output_path"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Glue Job Started")

secrets_client = boto3.client("secretsmanager")
secret_response = secrets_client.get_secret_value(
    SecretId=args["secret_name"]
)

secret = json.loads(secret_response["SecretString"])

jdbc_url = f"jdbc:mysql://{secret['host']}:{secret['port']}/{secret['database']}"

connection_properties = {
    "user": secret["username"],
    "password": secret["password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table=args["db_table"],
    properties=connection_properties
)

print("MySQL table loaded")
df.show()

transformed_df = df.withColumn(
    "total_price",
    col("quantity").cast("int") * col("price").cast("double")
)

transformed_df.show()

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(args["output_path"])

print("Output written to S3")

job.commit()

print("Glue Job Completed Successfully")
