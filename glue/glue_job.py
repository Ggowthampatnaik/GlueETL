import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "rds_endpoint",
        "db_name",
        "db_username",
        "db_password"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Glue Job Started")

jdbc_url = f"jdbc:mysql://{args['rds_endpoint']}:3306/{args['db_name']}"

connection_properties = {
    "user": args["db_username"],
    "password": args["db_password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="glue_demo",
    properties=connection_properties
)

print("MySQL Table Loaded")
df.show()

transformed_df = df.withColumn(
    "total_price",
    col("quantity").cast("int") * col("price").cast("double")
)

transformed_df.show()

output_path = "s3://glue-etl-mlops-pipeline/output/"

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("Output Written to S3")

job.commit()

print("Glue Job Completed Successfully")
