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
        "secret_id_rds",
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

def establish_connection_rds():
    print("Getting RDS secret", flush=True)

    secret_id = args["secret_id_rds"]

    sm_client = boto3.client("secretsmanager")
    response = sm_client.get_secret_value(SecretId=secret_id)

    print("Secret retrieved", flush=True)

    values = json.loads(response["SecretString"])

    host = values["host"]
    port = values["port"]
    dbname = values["database"]
    username = values["username"]
    password = values["password"]

    jdbc_url = (
        f"jdbc:mysql://{host}:{port}/{dbname}"
        "?useSSL=false"
        "&connectTimeout=10000"
        "&socketTimeout=30000"
    )

    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print("JDBC URL created", flush=True)

    return jdbc_url, connection_properties

jdbc_url, connection_properties = establish_connection_rds()

print("Reading MySQL table")

df = spark.read.jdbc(
    url=jdbc_url,
    table=args["db_table"],
    properties=connection_properties
)

print("Original data")
df.show()

print("Adding 1 to quantity")

transformed_df = df.withColumn(
    "quantity",
    col("quantity").cast("int") + 1
)

print("Transformed data")
transformed_df.show()

print("Writing transformed data to S3")

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(args["output_path"])

print("Glue Job Completed Successfully")

job.commit()
