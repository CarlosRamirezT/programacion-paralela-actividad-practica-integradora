import time

# Para una simulación simple sin PySpark instalado en el entorno.
# Si quieres usar PySpark local de verdad:
#   from pyspark.sql import SparkSession
#   ... implementar pipelines RDD y DataFrame reales.

def run_local_spark_job(mode: str, data):
    """
    Simulación de dos pipelines:
    - RDD: recorre lista y hace transformaciones
    - DF: simula operación vectorizada más eficiente
    Devuelve {"mode":..., "records": N}
    """
    n = len(data or [])
    if n == 0:
        return {"mode": mode, "records": 0}

    # Simulación: mismo resultado, distinto costo aproximado
    if mode == "rdd":
        t0 = time.time()
        # pipeline de múltiples pasos (simulado)
        acc = 0.0
        for x in data:
            y = (x * 1.5) + 2.0
            z = y / 3.0
            acc += z if z > 0.1 else 0.0
        # “resultado” = cantidad de registros
        _ = acc
        return {"mode": "rdd", "records": n}
    elif mode == "df":
        # Simula vectorización (más rápida)
        # sin dependencias extra
        acc = sum(((x * 1.5) + 2.0) / 3.0 for x in data)
        _ = acc
        return {"mode": "df", "records": n}
    else:
        return {"mode": mode, "records": n}
