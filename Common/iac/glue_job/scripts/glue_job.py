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
        "db_password",
        "db_table",
        "output_path",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

jdbc_url = f"jdbc:mysql://{args['rds_endpoint']}:3306/{args['db_name']}?useSSL=false"

connection_properties = {
    "user": args["db_username"],
    "password": args["db_password"],
    "driver": "com.mysql.cj.jdbc.Driver",
}

df = spark.read.jdbc(
    url=jdbc_url,
    table=args["db_table"],
    properties=connection_properties,
)

transformed_df = df.withColumn(
    "quantity",
    col("quantity").cast("int") + 1,
)

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(args["output_path"])

job.commit()
