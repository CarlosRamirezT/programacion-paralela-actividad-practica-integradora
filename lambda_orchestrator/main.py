import json
import os
import boto3
import pykka

from actors.validator import ValidatorActor
from actors.gpu_invoker import GpuInvokerActor
from actors.spark_invoker import SparkInvokerActor
from actors.aggregator import AggregatorActor

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GPU_LAMBDA_NAME = os.getenv("GPU_LAMBDA_NAME")
SPARK_LAMBDA_NAME = os.getenv("SPARK_LAMBDA_NAME")
SIMULATION_MODE = os.getenv("SIMULATION_MODE", "false").lower() == "true"

lambda_client = boto3.client("lambda", region_name=AWS_REGION)

def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        numbers = body.get("numbers", [])
        dataset_key = body.get("dataset_key", "data/sample_numbers.json")  # opcional

        # Iniciar el sistema de actores
        actor_system = pykka.ThreadingActor  # backend por defecto

        validator = ValidatorActor.start(numbers=numbers)
        gpu_invoker = GpuInvokerActor.start(
            lambda_client=lambda_client,
            gpu_lambda=GPU_LAMBDA_NAME,
            bucket=BUCKET_NAME,
            simulation=SIMULATION_MODE,
        )
        spark_invoker = SparkInvokerActor.start(
            lambda_client=lambda_client,
            spark_lambda=SPARK_LAMBDA_NAME,
            simulation=SIMULATION_MODE,
        )
        aggregator = AggregatorActor.start()

        # Flujo: validar → GPU → Spark (RDD + DF) → agregar
        valid = validator.ask({"cmd": "validate"})
        if not valid.get("ok"):
            raise ValueError(valid.get("error", "Invalid input"))

        gpu_res = gpu_invoker.ask({"cmd": "normalize", "numbers": numbers, "dataset_key": dataset_key})
        if not gpu_res.get("ok"):
            raise RuntimeError(gpu_res.get("error", "GPU step failed"))

        spark_rdd = spark_invoker.ask({"cmd": "spark_rdd", "input_key": gpu_res["output_key"]})
        spark_df  = spark_invoker.ask({"cmd": "spark_df",  "input_key": gpu_res["output_key"]})

        result = aggregator.ask({
            "cmd": "aggregate",
            "gpu": gpu_res,
            "rdd": spark_rdd,
            "df": spark_df,
        })

        # Apagar actores
        for ref in (validator, gpu_invoker, spark_invoker, aggregator):
            ref.stop()

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": str(e)}),
        }
