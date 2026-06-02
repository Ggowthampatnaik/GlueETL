#!/usr/bin/env python3

import aws_cdk as cdk
from jobs.glue_job_stack import StoreOpsGlueJobStack

app = cdk.App()

env_name = app.node.try_get_context("env")
config = app.node.try_get_context(env_name)

aws_env = cdk.Environment(
    account=config["account_id"],
    region=config["region"],
)

stack = StoreOpsGlueJobStack(
    app,
    "StoreOpsGlueJobStack",
    env_name=env_name,
    env=aws_env,
)

app.synth()
