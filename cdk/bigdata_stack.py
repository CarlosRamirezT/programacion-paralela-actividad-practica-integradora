from aws_cdk import (
    Stack, Duration, CfnOutput,
    aws_lambda as _lambda,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
import os
from pathlib import Path

class BigDataStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        bucket = s3.Bucket(self, "DataBucket")

        # Helper to package local code folder as asset
        def lambda_from_folder(name: str, folder: str, handler: str, memory=1024, timeout=30, env=None):
            code_path = str((Path(__file__).parents[1] / folder).resolve())
            fn = _lambda.Function(
                self, name,
                runtime=_lambda.Runtime.PYTHON_3_11,
                handler=handler,
                code=_lambda.Code.from_asset(code_path),
                memory_size=memory,
                timeout=Duration.seconds(timeout),
                environment=env or {},
            )
            bucket.grant_read_write(fn)
            return fn

        orchestrator = lambda_from_folder(
            "OrchestratorFn",
            "lambda_orchestrator",
            "main.handler",
            memory=1536,
            timeout=60,
            env={
                "BUCKET_NAME": bucket.bucket_name,
                "GPU_LAMBDA_NAME": "GpuFn",
                "SPARK_LAMBDA_NAME": "SparkFn",
                "AWS_REGION": self.region,
                "SIMULATION_MODE": "false",  # cámbialo a true para pruebas locales
            },
        )

        gpu_fn = lambda_from_folder(
            "GpuFn",
            "lambda_gpu",
            "main.handler",
            memory=2048,
            timeout=60,
            env={
                "BUCKET_NAME": bucket.bucket_name,
                "AWS_REGION": self.region,
            },
        )

        spark_fn = lambda_from_folder(
            "SparkFn",
            "lambda_spark",
            "main.handler",
            memory=2048,
            timeout=120,
            env={
                "BUCKET_NAME": bucket.bucket_name,
                "AWS_REGION": self.region,
                "EMR_APP_ID": "",         # set en producción
                "EMR_JOB_ROLE_ARN": "",   # set en producción
            },
        )

        # Permisos para que el orquestador invoque a los otros lambdas
        gpu_fn.grant_invoke(orchestrator)
        spark_fn.grant_invoke(orchestrator)

        http_api = apigwv2.HttpApi(self, "HttpApi", api_name="hybrid-actors-api", create_default_stage=True)
        integration = integrations.HttpLambdaIntegration("OrchIntegration", handler=orchestrator)
        http_api.add_routes(path="/process", methods=[apigwv2.HttpMethod.POST], integration=integration)

        CfnOutput(self, "ApiUrl", value=http_api.apiEndpoint)
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
