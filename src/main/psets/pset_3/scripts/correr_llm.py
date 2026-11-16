"""Llena la caché de respuestas de Gemini para la submuestra del notebook 03, sin bloquearlo.

Uso (en el contenedor, desde pset_3/):
    python scripts/correr_llm.py > /datos/llm.log

Usa la misma submuestra (`llm.muestra_evaluacion`) y la misma caché que el notebook 03: al
re-ejecutar el notebook, Gemini se lee de la caché y no vuelve a llamar a la API. Reintenta con
paciencia ante 503/429 del nivel gratuito. No usa GPU.
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ / "notebooks")

from dotenv import load_dotenv  # noqa: E402

from src import datos, llm, pipeline as pl  # noqa: E402

load_dotenv("../.env")
DATOS = Path(os.environ.get("DATOS_DIR", "/datos"))
manifiesto = datos.leer_manifiesto(DATOS / "obt_mc1")
train = datos.cargar_cache(DATOS / "cache_R", hasta=datos.FIN_ENTRENAMIENTO, sha_esperado=manifiesto["dataset_sha256"])
meta = train[["SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D"]]
splitter = pl.EmbargoTimeSeriesSplit(n_splits=4, meses_validacion=2, embargo_dias=30)
pos = llm.muestra_evaluacion(meta, splitter, seed=pl.SEMILLA)
r = llm.predecir(train.iloc[pos], DATOS / "llm_cache" / llm.MODELO_LLM, seed=pl.SEMILLA)
print(f"listo: {r.llamadas_api} llamadas, {r.respuestas_cache} desde caché, lotes fallidos {r.lotes_fallidos}, "
      f"tokens {r.tokens_entrada}/{r.tokens_salida}/{r.tokens_razonamiento}", flush=True)
