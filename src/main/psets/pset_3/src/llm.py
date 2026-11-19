"""Familia "LLM por prompting": Gemini estima la probabilidad de falla a 30 días de un SSD.

No se entrena nada: el modelo recibe, por cada disco-día, un resumen de sus atributos SMART más
informativos (valor del día, media de 30 días y cambios de 7 y 30 días; ranking de la EDA) y
devuelve una probabilidad en JSON con esquema fijo.

Diseño para el nivel gratuito de la API de Gemini:
- varios casos por solicitud (`casos_por_lote`), para hacer pocas llamadas;
- cada respuesta se guarda en disco con la clave SHA-256 de (modelo, instrucciones, lote): repetir
  la corrida no vuelve a llamar a la API y da exactamente el mismo resultado;
- reintentos con espera exponencial ante 429 (límite de frecuencia) y errores del servidor.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

MODELO_LLM = "gemini-3.8-flash"

# Atributos y ventanas que se muestran al LLM (mejores en PR-AUC por evento en la EDA).
ATRIBUTOS_LLM = (198, 5, 187, 197, 9, 1, 173, 180, 12, 199)
VENTANAS_LLM = (("hoy", ""), ("media_30d", "_MEAN_30D"), ("cambio_7d", "_MEAN_SHIFT_7D"),
                ("cambio_30d", "_MEAN_SHIFT_30D"))

INSTRUCCIONES = """You are a reliability engineer for a large data-center fleet of SSDs (Alibaba, model MC1).
For each drive-day you receive SMART telemetry. Estimate the probability that the drive FAILS within
the next 30 days.

Fields per attribute: "hoy" = raw value today; "media_30d" = mean over the last 30 days;
"cambio_7d" / "cambio_30d" = mean of the last 7 (30) days minus the mean of the 7 (30) days before
(positive = the counter is growing). "edad_dias" = days since the drive was first observed.
Attributes: 198 = offline uncorrectable sectors, 5 = reallocated sectors, 187 = reported
uncorrectable errors, 197 = pending sectors, 9 = power-on hours, 1 = raw read error rate,
173 = wear-leveling count, 180 = unused reserved blocks (lower = worse), 12 = power cycles,
199 = CRC errors. Missing values are null.

Base rate: about 0.25% of drive-days precede a failure within 30 days, so most drives are healthy.
Growing error/sector counters and old drives are warning signs. Return one probability between 0
and 1 for every id you receive, calibrated to that base rate."""


# Columnas de la OBT que lee el LLM (las únicas que entran al prompt)
COLUMNAS_LLM = ["DAYS_SINCE_FIRST_SEEN"] + [f"R_{a}{sufijo}" for a in ATRIBUTOS_LLM for _, sufijo in VENTANAS_LLM]


def modelo_pyfunc(cache_dir: str | Path, modelo: str = MODELO_LLM, seed: int = 42):
    """El LLM como modelo de MLflow (pyfunc): recibe un DataFrame con COLUMNAS_LLM y devuelve la
    probabilidad de falla. Llama a la API de Gemini (GEMINI_API_KEY) salvo que la respuesta esté en
    `cache_dir`; así queda registrado con firma y ejemplo de entrada como las demás familias."""
    import mlflow.pyfunc  # noqa: PLC0415

    class ModeloLLM(mlflow.pyfunc.PythonModel):
        def __init__(self, cache_dir, modelo, seed):
            self.cache_dir, self.modelo, self.seed = str(cache_dir), modelo, seed

        def predict(self, context, model_input, params=None):
            from src import llm as _llm  # noqa: PLC0415 - se resuelve con code_paths al cargar
            return _llm.predecir(model_input, self.cache_dir, modelo=self.modelo, seed=self.seed).probabilidades

    return ModeloLLM(cache_dir, modelo, seed)


def resumen_caso(fila: pd.Series) -> dict:
    """Lo que ve el LLM de un disco-día: solo features (nunca etiquetas ni identificadores)."""
    def num(v):
        return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 3)

    caso = {"edad_dias": num(fila.get("DAYS_SINCE_FIRST_SEEN"))}
    for a in ATRIBUTOS_LLM:
        caso[f"smart_{a}"] = {nombre: num(fila.get(f"R_{a}{sufijo}")) for nombre, sufijo in VENTANAS_LLM}
    return caso


@dataclass
class ResultadoLLM:
    probabilidades: np.ndarray
    llamadas_api: int = 0
    respuestas_cache: int = 0
    tokens_entrada: int = 0
    tokens_salida: int = 0
    tokens_razonamiento: int = 0
    segundos_api: float = 0.0
    lotes_fallidos: list = field(default_factory=list)


def muestra_evaluacion(meta: pd.DataFrame, splitter, seed: int = 42, n_pos: int = 100,
                      n_neg: int = 400) -> np.ndarray:
    """Posiciones (en `meta`) de la submuestra estratificada del ÚLTIMO pliegue de validación.

    La usan el notebook 03 y scripts/correr_llm.py: mismos casos, misma caché.
    """
    _, iva = list(splitter.split(meta))[-1]
    y = meta["TARGET_30D"].to_numpy()[iva]
    rng = np.random.default_rng(seed)
    sel = np.sort(np.concatenate([rng.choice(np.flatnonzero(y == 1), n_pos, replace=False),
                                  rng.choice(np.flatnonzero(y == 0), n_neg, replace=False)]))
    return iva[sel]


def _clave(modelo: str, lote: list[dict]) -> str:
    texto = json.dumps({"modelo": MODELO_LLM if modelo is None else modelo, "instr": INSTRUCCIONES,
                        "lote": lote}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(texto.encode()).hexdigest()


def predecir(X: pd.DataFrame, cache_dir: str | Path, *, modelo: str = MODELO_LLM,
             casos_por_lote: int = 20, pausa_s: float = 6.0, max_reintentos: int = 25,
             timeout_s: int = 600, seed: int = 42, api_key: str | None = None) -> ResultadoLLM:
    """Probabilidad de falla para cada fila de X (en el mismo orden).

    `pausa_s` espacia las llamadas (6 s ≈ 10 solicitudes/min) para no tocar el límite gratuito.
    Ante 429 (límite de frecuencia) o 5xx (p. ej. 503 "high demand" del nivel gratuito) se
    reintenta con espera exponencial de hasta 15 min; `timeout_s` corta una llamada colgada.
    """
    from google import genai  # noqa: PLC0415
    from google.genai import errors, types  # noqa: PLC0415
    from pydantic import BaseModel  # noqa: PLC0415

    class Prediccion(BaseModel):
        id: int
        probabilidad: float

    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cliente = None
    config = types.GenerateContentConfig(
        system_instruction=INSTRUCCIONES, temperature=0.0, seed=seed,
        response_mime_type="application/json", response_schema=list[Prediccion])

    casos = [resumen_caso(f) for _, f in X.iterrows()]
    probs = np.full(len(casos), np.nan)
    res = ResultadoLLM(probs)
    for inicio in range(0, len(casos), casos_por_lote):
        lote = [{"id": i, **casos[i]} for i in range(inicio, min(inicio + casos_por_lote, len(casos)))]
        ruta = cache_dir / f"{_clave(modelo, lote)}.json"
        if ruta.exists():
            guardado = json.loads(ruta.read_text())
            res.respuestas_cache += 1
        else:
            if cliente is None:
                cliente = genai.Client(api_key=api_key or os.environ["GEMINI_API_KEY"],
                                       http_options=types.HttpOptions(timeout=timeout_s * 1000))
            contenido = "Drive-days (JSON):\n" + json.dumps(lote, ensure_ascii=False)
            for intento in range(max_reintentos):
                try:
                    t0 = time.perf_counter()
                    r = cliente.models.generate_content(model=modelo, contents=contenido, config=config)
                    seg = time.perf_counter() - t0
                    break
                except (errors.ClientError, errors.ServerError) as e:
                    reintentable = isinstance(e, errors.ServerError) or getattr(e, "code", None) == 429
                    if not reintentable or intento == max_reintentos - 1:
                        raise
                    espera = min(30 * 2 ** intento, 900)
                    print(f"Gemini {getattr(e, 'code', '?')}: reintento {intento + 1} en {espera} s", flush=True)
                    time.sleep(espera)
                except Exception as e:  # noqa: BLE001 - timeout o corte de red: reintentar igual
                    if intento == max_reintentos - 1:
                        raise
                    print(f"Gemini {type(e).__name__}: reintento {intento + 1}", flush=True)
                    time.sleep(min(30 * 2 ** intento, 900))
            u = r.usage_metadata
            guardado = {"respuesta": r.text, "segundos": seg,
                        "tokens": [u.prompt_token_count or 0, u.candidates_token_count or 0,
                                   getattr(u, "thoughts_token_count", 0) or 0]}
            ruta.write_text(json.dumps(guardado, ensure_ascii=False))
            res.llamadas_api += 1
            res.segundos_api += seg
            res.tokens_entrada += guardado["tokens"][0]
            res.tokens_salida += guardado["tokens"][1]
            res.tokens_razonamiento += guardado["tokens"][2]
            time.sleep(pausa_s)
        try:
            for p in json.loads(guardado["respuesta"]):
                if lote[0]["id"] <= int(p["id"]) <= lote[-1]["id"]:
                    probs[int(p["id"])] = min(max(float(p["probabilidad"]), 0.0), 1.0)
        except (ValueError, KeyError, TypeError):
            res.lotes_fallidos.append(lote[0]["id"])
    return res
