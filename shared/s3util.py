import json
import boto3
import os

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

def put_json(bucket, key, obj):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj).encode("utf-8"), ContentType="application/json")
    return f"s3://{bucket}/{key}"

def get_json(bucket, key):
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))
