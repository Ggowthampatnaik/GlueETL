import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# ---------------------------------------------------
# Custom Transform
# ---------------------------------------------------

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = df.withColumn("quantity", col("quantity") + 1)

    df = df.coalesce(1)

    result = DynamicFrame.fromDF(df, glueContext, "result")

    return DynamicFrameCollection({"CustomTransform0": result}, glueContext)


# ---------------------------------------------------
# Glue Job Init
# ---------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# ---------------------------------------------------
# Data Quality Rules
# ---------------------------------------------------

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# ---------------------------------------------------
# Read from MySQL RDS using Glue Connection
# ---------------------------------------------------

MySQL_node = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "glue_demo",
        "connectionName": "mysql-rds-glue-connection",
    },
    transformation_ctx="MySQL_node"
)

# ---------------------------------------------------
# Custom Transform
# ---------------------------------------------------

CustomTransform_node = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"MySQL_node": MySQL_node},
        glueContext
    )
)

# ---------------------------------------------------
# Select From Collection
# ---------------------------------------------------

SelectFromCollection_node = SelectFromCollection.apply(
    dfc=CustomTransform_node,
    key=list(CustomTransform_node.keys())[0],
    transformation_ctx="SelectFromCollection_node"
)

# ---------------------------------------------------
# Data Quality Check
# ---------------------------------------------------

EvaluateDataQuality().process_rows(
    frame=SelectFromCollection_node,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# ---------------------------------------------------
# Write to S3
# ---------------------------------------------------

AmazonS3_node = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://glue-etl-mlops-pipeline/output/",
        "partitionKeys": []
    },
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="AmazonS3_node"
)

job.commit()
