import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "rds_endpoint",
        "db_name",
        "db_username",
        "db_password",
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

jdbc_url = (
    f"jdbc:mysql://{args['rds_endpoint']}:3306/"
    f"{args['db_name']}?useSSL=false"
)

connection_properties = {
    "user": args["db_username"],
    "password": args["db_password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}

print("Reading MySQL Table")

df = spark.read.jdbc(
    url=jdbc_url,
    table=args["db_table"],
    properties=connection_properties
)

df.show()

print("Writing Output to S3")

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(args["output_path"])

print("Glue Job Completed")

job.commit()
