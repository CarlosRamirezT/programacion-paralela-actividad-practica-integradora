# Híbrido Big-Data Serverless con Pykka (Actores) + GPU + Spark

Proyecto de referencia:
- Orquestación con actores (Pykka) dentro de un Lambda "orchestrator".
- Preprocesamiento "GPU" (normalización). Si no hay GPU (CUDA), cae a CPU paralela con Numba.
- Procesamiento de Spark:
  - Producción: disparo de EMR Serverless (boto3).
  - Local: simulación con PySpark (RDD y DataFrame) y cálculo de speedup.
- Despliegue con AWS CDK (Python): API Gateway + Lambdas.

## Variables de entorno (Lambdas)
- **Comunes**:
  - `AWS_REGION`
  - `BUCKET_NAME` (S3 donde guardar/leer artefactos)
- **Orchestrator**:
  - `GPU_LAMBDA_NAME` (nombre lógico del Lambda GPU)
  - `SPARK_LAMBDA_NAME` (nombre lógico del Lambda Spark)
  - `SIMULATION_MODE` (`true|false`) para ejecutar todo local/simulado
- **Spark Lambda**:
  - `EMR_APP_ID` (EMR Serverless Application ID) — si vas a producción
  - `EMR_JOB_ROLE_ARN` (rol de ejecución del job en EMR Serverless)
  - `SPARK_ENTRY_S3_URI` (ruta S3 del script principal, si aplicara) — opcional

## Flujo
1) POST `/process`
2) Orchestrator valida input → invoca GPU Lambda (normaliza) → sube/usa S3
3) Orchestrator invoca Spark Lambda con ruta S3 del dataset → Spark procesa:
   - Pipeline RDD
   - Pipeline DataFrame
   - Calcula speedup aproximado
4) Orchestrator agrega resultados y responde JSON.

## Despliegue rápido (infra mínima)

```bash
cd cdk
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cdk bootstrap aws://<ACCOUNT>/<REGION>
cdk deploy
```

# Limpieza

```bash
cd cdk
cdk destroy
```

