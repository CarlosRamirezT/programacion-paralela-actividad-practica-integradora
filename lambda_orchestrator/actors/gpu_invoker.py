import json
import pykka

class GpuInvokerActor(pykka.ThreadingActor):
    def __init__(self, lambda_client, gpu_lambda, bucket, simulation=False):
        super().__init__()
        self.lambda_client = lambda_client
        self.gpu_lambda = gpu_lambda
        self.bucket = bucket
        self.simulation = simulation

    def on_receive(self, message):
        if message.get("cmd") != "normalize":
            return {"ok": False, "error": "unknown command"}

        payload = {
            "bucket": self.bucket,
            "numbers": message.get("numbers", []),
            "dataset_key": message.get("dataset_key") or "data/preprocessed.json",
            "simulation": self.simulation,
        }

        resp = self.lambda_client.invoke(
            FunctionName=self.gpu_lambda,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        body = json.loads(resp["Payload"].read().decode("utf-8"))
        return body
