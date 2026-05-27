import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Glue Job Started")

mysql_df = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "glue_demo",
        "connectionName": "mysql-rds-glue-connection",
    },
    transformation_ctx="mysql_df"
)

print("MySQL Data Loaded")
mysql_df.show()

spark_df = mysql_df.toDF()

transformed_df = spark_df.withColumn(
    "total_price",
    spark_df["quantity"].cast("int") * spark_df["price"].cast("double")
)

output_path = "s3://glue-etl-mlops-pipeline/output/"

transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("Output Written to S3")

job.commit()

print("Glue Job Completed Successfully")
