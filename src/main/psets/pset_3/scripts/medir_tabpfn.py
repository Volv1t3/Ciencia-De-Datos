"""Mide el costo de TabPFN (versiones y tamaños de contexto) en el pliegue de CV más grande.

Uso (en el contenedor):  python scripts/medir_tabpfn.py > /datos/medicion_tabpfn.log
Corre sin token: carga los pesos desde TABPFN_MODEL_DIR (descargados una vez). Imprime una línea
JSON por combinación: segundos de fit, de predecir N_PRED filas y la extrapolación a un pliegue.
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np  # noqa: E402
from sklearn.metrics import average_precision_score  # noqa: E402

from src import datos, pipeline as pl  # noqa: E402

os.environ.pop("TABPFN_TOKEN", None)  # demostrar que basta con los pesos locales
from tabpfn import TabPFNClassifier  # noqa: E402

DIR_PESOS = Path(os.getenv("TABPFN_MODEL_DIR", "/datos/modelos/tabpfn"))
VERSIONES = {"v3.5": "tabpfn-v3.5-20260909.safetensors"}
N_PRED = int(os.getenv("N_PRED", "50000"))
CONTEXTOS = [int(x) for x in os.getenv("CONTEXTOS", "20000,100000").split(",")]

df = datos.cargar_cache(Path(os.getenv("DATOS_DIR", "/datos")) / "cache_R", hasta=datos.FIN_ENTRENAMIENTO)
meta = df[["SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D"]]
cols = pl.columnas_modelo(df, "R")
idx = df.columns.get_indexer(cols)
itr, iva = list(pl.EmbargoTimeSeriesSplit().split(meta))[-1]
y = meta["TARGET_30D"].to_numpy()
rng = np.random.default_rng(pl.SEMILLA)
pos, neg = itr[y[itr] == 1], itr[y[itr] == 0]
base = np.concatenate([rng.choice(pos, min(len(pos), max(CONTEXTOS) // 2), replace=False),
                       rng.choice(neg, max(CONTEXTOS), replace=False)])
prep = pl.construir_preprocesador("tabpfn").fit(df.iloc[base, idx])
Xb, yb = prep.transform(df.iloc[base, idx]), y[base]
val = rng.choice(iva, N_PRED, replace=False)
Xv, yv = prep.transform(df.iloc[val, idx]), y[val]
print(json.dumps({"features": int(Xb.shape[1]), "filas_val_pliegue": int(len(iva)), "n_pred": N_PRED}), flush=True)

versiones = [(v, DIR_PESOS / f) for v, f in VERSIONES.items()]
versiones += [("v3.5-fast", p) for p in sorted(DIR_PESOS.glob("*3.5-fast*.safetensors"))[:1]]
for version, ruta in versiones:
    for n in CONTEXTOS:
        sel = np.concatenate([rng.choice(np.flatnonzero(yb == 1), n // 2, replace=False),
                              rng.choice(np.flatnonzero(yb == 0), n - n // 2, replace=False)])
        try:
            clf = TabPFNClassifier(device="cuda", random_state=pl.SEMILLA, model_path=str(ruta))
            t = time.time(); clf.fit(Xb[sel], yb[sel]); tf = time.time() - t
            t = time.time(); p = clf.predict_proba(Xv)[:, 1]; tp = time.time() - t
            print(json.dumps({"version": version, "contexto": n, "fit_s": round(tf, 1), "pred_s": round(tp, 1),
                              "min_por_pliegue": round(tp * len(iva) / N_PRED / 60, 1),
                              "ap_fila_muestra": round(float(average_precision_score(yv, p)), 5)}), flush=True)
        except Exception as e:  # noqa: BLE001
            print(json.dumps({"version": version, "contexto": n, "error": f"{type(e).__name__}: {e}"[:400]}), flush=True)
