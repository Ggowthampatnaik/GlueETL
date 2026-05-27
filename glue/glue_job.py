import sys
import json
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "secret_name", "db_table", "output_path"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Glue Job Started")

client = boto3.client("secretsmanager")
secret_response = client.get_secret_value(SecretId=args["secret_name"])
secret = json.loads(secret_response["SecretString"])

host = secret["host"]
port = secret["port"]
database = secret["database"]
username = secret["username"]
password = secret["password"]

print("Secret loaded")
print(f"Host: {host}")
print(f"Database: {database}")
print(f"Table: {args['db_table']}")

jdbc_url = f"jdbc:mysql://{host}:{port}/{database}?connectTimeout=10000&socketTimeout=30000&useSSL=false"

connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": "1000"
}

print("Testing MySQL connection with COUNT query")

count_query = f"(SELECT COUNT(*) AS row_count FROM {args['db_table']}) AS count_check"

count_df = spark.read.jdbc(
    url=jdbc_url,
    table=count_query,
    properties=connection_properties
)

count_df.show()

print("Reading table")

df = spark.read.jdbc(
    url=jdbc_url,
    table=args["db_table"],
    properties=connection_properties
)

df.show(10)

print("Writing output")

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(args["output_path"])

print("Data Written to S3")

job.commit()

print("Glue Job Completed")
