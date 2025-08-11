import json
import os
import time
import numpy as np
from gpu import normalize_gpu_aware
from shared.s3util import put_json

BUCKET = os.getenv("BUCKET_NAME")

def handler(event, context):
    try:
        numbers = event.get("numbers", [])
        dataset_key = event.get("dataset_key", "data/preprocessed.json")
        simulation = bool(event.get("simulation", False))

        arr = np.array(numbers, dtype=np.float32)
        t0 = time.time()
        norm, method = normalize_gpu_aware(arr, simulation=simulation)
        elapsed = int((time.time() - t0) * 1000)

        payload = {
            "ok": True,
            "output_key": dataset_key,
            "count": int(norm.size),
            "method": method,
            "time_ms": elapsed,
        }

        # Guardar a S3
        put_json(BUCKET, dataset_key, {"data": norm.tolist()})
        return payload
    except Exception as e:
        return {"ok": False, "error": str(e)}
