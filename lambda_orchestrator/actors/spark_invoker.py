import json
import pykka

class SparkInvokerActor(pykka.ThreadingActor):
    def __init__(self, lambda_client, spark_lambda, simulation=False):
        super().__init__()
        self.lambda_client = lambda_client
        self.spark_lambda = spark_lambda
        self.simulation = simulation

    def _call(self, mode: str, input_key: str):
        payload = {"mode": mode, "input_key": input_key, "simulation": self.simulation}
        resp = self.lambda_client.invoke(
            FunctionName=self.spark_lambda,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        return json.loads(resp["Payload"].read().decode("utf-8"))

    def on_receive(self, message):
        cmd = message.get("cmd")
        if cmd == "spark_rdd":
            return self._call("rdd", message["input_key"])
        if cmd == "spark_df":
            return self._call("df", message["input_key"])
        return {"ok": False, "error": "unknown command"}
