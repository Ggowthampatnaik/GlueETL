#!/usr/bin/env python3

import os

import aws_cdk as cdk

from connectors.glue_connectors_stack import StoreOpsGlueConnectionsStack

app = cdk.App()

env = app.node.try_get_context("env")
aws_account = app.node.try_get_context(env)["account_id"]

storeops_env = cdk.Environment(
    account=aws_account,
    region=app.node.try_get_context(env)["region"],
)

topics = StoreOpsGlueConnectionsStack(
    app,
    construct_id="StoreOpsGlueConnectionsStack",
    env_name=env,
    env=storeops_env,
)

cdk.Tags.of(topics).add(
    "signet:environment",
    env,
    priority=101,
)

cdk.Tags.of(topics).add(
    "signet:created-by",
    "CDK",
    priority=101,
)

cdk.Tags.of(topics).add(
    "signet:sap-id",
    "IT.24.0009",
    priority=101,
)

cdk.Tags.of(topics).add(
    "signet:app-id",
    "edl",
    priority=101,
)

app.synth()
