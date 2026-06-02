from aws_cdk import (
    Aws,
    CfnParameter,
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class StoreOpsGlueJobStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        config = self.node.try_get_context(env_name)

        db_password = CfnParameter(
            self,
            "DbPassword",
            type="String",
            no_echo=True,
            description="RDS database password",
        )

        script_bucket = s3.Bucket.from_bucket_name(
            self,
            "GlueScriptBucket",
            config["s3_bucket_name"],
        )

        s3deploy.BucketDeployment(
            self,
            "DeployGlueScript",
            sources=[
                s3deploy.Source.asset("scripts")
            ],
            destination_bucket=script_bucket,
            destination_key_prefix="glue-scripts",
        )

        glue.CfnJob(
            self,
            "GlueETLJob",
            name=config["glue_job_name"],
            role=config["glue_role_arn"],
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{config['s3_bucket_name']}/glue-scripts/glue_job.py",
                python_version="3",
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[
                    config["network_connection_name"]
                ]
            ),
            default_arguments={
                "--job-language": "python",
                "--rds_endpoint": config["rds_endpoint"],
                "--db_name": config["db_name"],
                "--db_username": config["db_username"],
                "--db_password": db_password.value_as_string,
                "--db_table": config["db_table"],
                "--output_path": config["output_path"],
            },
        )
