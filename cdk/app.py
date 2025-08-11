#!/usr/bin/env python3
import os
import aws_cdk as cdk
from bigdata_stack import BigDataStack

app = cdk.App()
BigDataStack(
    app,
    "HybridActorsServerlessStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION") or "us-east-1",
    ),
)
app.synth()
