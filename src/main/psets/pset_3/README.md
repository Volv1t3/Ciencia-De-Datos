# PSet #3 — Experimentación y selección del modelo (fallas de SSD MC1)

Clasificador de falla a 30 días para SSD del modelo MC1. Parte de la OBT que construyó el PSet #2
en Snowflake y termina con un modelo en el Model Registry de MLflow (`ssd-failure-modelo@champion`).

```
OBT (Snowflake) → snapshot Parquet → EDA → features → Pipeline → baselines → candidatos → tuning → selección → registry
```

## Definiciones que vienen del PSet #1

| Elemento | Definición |
|---|---|
| Unidad | SSD-día (`SSD_KEY`, `OBSERVATION_DATE`) del modelo MC1 |
| Target | `TARGET_30D`: falla entre 1 y 30 días después, con seguimiento completo |
| Episodio de alerta | Uno por disco: la primera fecha con score ≥ τ. Es **acierto** si el disco falla entre **7 y 30 días** después |
| Métrica técnica | **PR-AUC por evento** (selección, CV y tuning) y **F0,5 por evento** (operación). El recall se cuenta sobre discos que fallan, no sobre filas |
| Baselines | Dummy · regla de umbral sobre el mejor atributo SMART · modelo sin features temporales |
| Métrica de negocio | Costo esperado por 1.000 SSD-mes con C_FP = 1 y C_FN = k, para k ∈ [1, 50], y el k de equilibrio frente a la regla |
| Partición | Train/CV: 2018-01 → 2019-09 · embargo: 2019-10 · prueba: 2019-11. CV temporal con 4 pliegues y 30 días de embargo |

## Requisitos de hardware

Todo el experimento usa la OBT completa (~115M filas SSD-día; las que tienen etiqueta se descargan).

| Recurso | Mínimo | Motivo |
|---|---|---|
| GPU | NVIDIA con ≥ 80 GB (se usó un H200 de 141 GB) | XGBoost, cuML, la MLP y TabPFN en CUDA |
| RAM del host | ≥ 512 GB | La variante R en float32 ocupa ~200 GB y la CV hace copias por pliegue |
| Disco | ≥ 400 GB libres | Snapshot Parquet y artefactos de MLflow |
| Software | Docker con `nvidia-container-toolkit` y un driver compatible con la imagen de RAPIDS | `docker/Dockerfile.gpu` |

Para comprobarlo en el servidor: `nvidia-smi`, `free -g`, `df -h` y `docker run --rm --gpus all nvidia/cuda:12.8.0-base-ubuntu24.04 nvidia-smi`.

## 1. Levantar MLflow y el entorno en el servidor

```bash
git clone <repo> && cd <repo>/src/main/psets/pset_3
cp .env.example .env          # completar credenciales, DATOS_DIR_HOST y tokens
docker compose up -d --build  # mlflow-postgres, mlflow, jupyter-gpu
docker compose ps             # los tres en "healthy"/"running"
curl -s localhost:5050/health # -> OK
```

- **Backend store:** Postgres (`mlflow-postgres`, volumen `mlflow_postgres_data`).
- **Artifact store:** volumen `mlflow_artifacts`, servido por el propio servidor (`--serve-artifacts`).
- Los puertos solo se publican en `127.0.0.1`. Para ver la UI y Jupyter desde tu máquina:

```bash
ssh -N -L 5050:localhost:5050 -L 8888:localhost:8888 usuario@servidor-h200
# MLflow: http://localhost:5050   ·   Jupyter: http://localhost:8888/?token=<JUPYTER_TOKEN>
```

Después del primer build, congela el entorno y versiónalo:
`docker compose exec jupyter-gpu pip freeze > requirements.lock.txt`.

## 2. Conexión a Snowflake

Variables en `.env`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`,
`SNOWFLAKE_WAREHOUSE` y `OBT_TABLA`. `OBT_TABLA` es el nombre completo `BASE.ESQUEMA.TABLA`; si se
lee desde un *share*, la base es la del share.

El snapshot se extrae una sola vez (lo hace el notebook 01, o a mano):

```python
from src import datos
datos.extraer_obt("/datos/obt_mc1")   # COPY INTO @~ (Parquet por mes) + GET + manifiesto.json
```

`manifiesto.json` guarda la tabla de origen, la fecha de corte, los conteos, el SHA-256 del SQL y
el SHA-256 del dataset. `datos.cargar_dataset()` verifica ese hash antes de entrenar, y cada
corrida de MLflow lo registra (`datos_sha256`, `datos_fecha_corte`, `datos_tabla_origen`).

## 3. Orden de ejecución de los notebooks

Ejecutar en Jupyter (contenedor `jupyter-gpu`) con **Restart & Run All**; la salida queda guardada.

| # | Notebook | Produce |
|---|---|---|
| 1 | `notebooks/01_eda.ipynb` | Snapshot, EDA, hallazgos, elección de variante (R/N/RN) y del atributo para la regla |
| 2 | `notebooks/02_features.ipynb` | Features derivadas, evidencia y prueba de no-fuga |
| 3 | `notebooks/03_baseline_candidatos.ipynb` | 3 baselines + 5 familias + LLM (`fase=baseline`/`candidato`) |
| 4 | `notebooks/04_tuning.ipynb` | Optuna, 30 trials × 3 modelos como corridas anidadas (`fase=tuning`) |
| 5 | `notebooks/05_seleccion.ipynb` | Prueba única, costo de negocio, registry `champion`/`challenger`, `docs/corridas.csv` |

La lógica vive en `src/` y los notebooks la importan:

- `src/datos.py`: extracción, manifiesto, hash, carga y partición con embargo.
- `src/pipeline.py`: preprocesamiento (`ColumnTransformer` + `Pipeline`), modelos, `EmbargoTimeSeriesSplit`, métricas por evento, costo de negocio y registro en MLflow.

## Pruebas

```bash
python -m pytest -q tests   # métricas por evento contra la definición literal, embargo, no-fuga, CV
```

## Estructura

```
pset_3/
├── docker-compose.yml   docker/        .env.example   requirements.txt   README.md
├── notebooks/           01_eda … 05_seleccion
├── src/                 datos.py  pipeline.py
├── tests/               test_pipeline.py
└── docs/                corridas.csv   figuras/   PSet3_memo_Arellano_Guaman_Perla_Teran.pdf
```
