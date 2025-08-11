import json
import os
import time
import boto3

from shared.s3util import get_json
from spark_job import run_local_spark_job

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("BUCKET_NAME")
EMR_APP_ID = os.getenv("EMR_APP_ID", "")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN", "")
emr = boto3.client("emr-serverless", region_name=AWS_REGION)

def handler(event, context):
    """
    event = { mode: "rdd"|"df", input_key: "s3 key", simulation: bool }
    """
    try:
        mode = event.get("mode", "rdd")
        key  = event.get("input_key")
        simulation = bool(event.get("simulation", False))

        if simulation or not EMR_APP_ID or not EMR_JOB_ROLE_ARN:
            # Simulación local: carga JSON desde S3 y procesa con PySpark local (si está disponible)
            data = get_json(BUCKET, key).get("data", [])
            t0 = time.time()
            res = run_local_spark_job(mode, data)
            elapsed = int((time.time() - t0) * 1000)
            res["time_ms"] = elapsed
            res["ok"] = True
            return res

        # Producción: disparar un job en EMR Serverless
        # (Suponiendo que tienes un entry point en S3 con tu script de Spark)
        # Aquí dejamos una plantilla minimalista; ajusta JobDriver y ConfigurationOverrides a tu caso.
        job = emr.start_job_run(
            applicationId=EMR_APP_ID,
            executionRoleArn=EMR_JOB_ROLE_ARN,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://path-to-your-spark-script.py",
                    "entryPointArguments": [f"--mode={mode}", f"--s3bucket={BUCKET}", f"--s3key={key}"],
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET}/emr-logs/"}
                }
            },
            name=f"spark-{mode}-job",
        )
        return {"ok": True, "job_run_id": job["jobRunId"], "records": None, "time_ms": None}
    except Exception as e:
        return {"ok": False, "error": str(e)}
