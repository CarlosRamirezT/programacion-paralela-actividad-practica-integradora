import pykka

class AggregatorActor(pykka.ThreadingActor):
    def on_receive(self, message):
        if message.get("cmd") != "aggregate":
            return {"ok": False, "error": "unknown command"}

        gpu = message.get("gpu", {})
        rdd = message.get("rdd", {})
        df  = message.get("df", {})

        out = {
            "ok": True,
            "pipeline": {
                "gpu": {
                    "output_key": gpu.get("output_key"),
                    "count": gpu.get("count"),
                    "method": gpu.get("method"),
                },
                "spark": {
                    "rdd": {
                        "records": rdd.get("records"),
                        "time_ms": rdd.get("time_ms"),
                    },
                    "df": {
                        "records": df.get("records"),
                        "time_ms": df.get("time_ms"),
                    },
                    "speedup_df_vs_rdd": None
                }
            }
        }

        t_rdd = rdd.get("time_ms") or 0
        t_df  = df.get("time_ms") or 0
        if t_rdd > 0 and t_df > 0:
            out["pipeline"]["spark"]["speedup_df_vs_rdd"] = round(t_rdd / t_df, 3)

        return out
