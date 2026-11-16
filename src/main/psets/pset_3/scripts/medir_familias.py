"""Mide el costo de cada familia en el pliegue de CV más grande (presupuesto del notebook 03/04).

Uso (en el contenedor):  python scripts/medir_familias.py [familia ...] > /datos/medicion.log
Escribe una línea JSON por familia: filas de entrenamiento, segundos de fit, de predicción en
validación y de la curva por evento, y la PR-AUC por evento del pliegue (solo orientativa).
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np  # noqa: E402

from src import datos, pipeline as pl  # noqa: E402

FAMILIAS = sys.argv[1:] or ["dummy", "regla", "sin_temporales", "logistica", "bosque", "xgboost", "mlp", "tabpfn"]
FRAC_SANOS = float(os.getenv("FRAC_SANOS", "0.15"))
DATOS = Path(os.getenv("DATOS_DIR", "/datos"))

t = time.time()
eda = json.loads((Path(__file__).resolve().parents[1] / "docs" / "eda_decisiones.json").read_text())
df = datos.cargar_cache(DATOS / "cache_R", hasta=datos.FIN_ENTRENAMIENTO)
cols = pl.columnas_modelo(df, eda["variante"])
meta = df[["SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D"]]  # sin las 419 features
itr, iva = list(pl.EmbargoTimeSeriesSplit().split(meta))[-1]
itr = itr[pl.muestra_por_disco(meta.iloc[itr], FRAC_SANOS)]
idx = df.columns.get_indexer(cols)
Xtr, ytr = df.iloc[itr, idx], meta["TARGET_30D"].to_numpy()[itr]
Xva, dva = df.iloc[iva, idx], meta.iloc[iva]
prep = pl.preparar_eventos(dva["SSD_KEY"], dva["OBSERVATION_DATE"], dva["DAYS_TO_FAILURE"])
print(json.dumps({"carga_s": round(time.time() - t, 1), "filas_train": len(itr), "filas_val": len(iva),
                  "positivas_train": int(ytr.sum()), "features": len(cols)}), flush=True)

for fam in FAMILIAS:
    params = {"atributo": eda["atributo_regla"]} if fam == "regla" else None
    try:
        m = pl.construir_modelo(fam, params)
        t0 = time.time(); m.fit(Xtr, ytr); t_fit = time.time() - t0
        t0 = time.time(); s = m.predict_proba(Xva)[:, 1]; t_pred = time.time() - t0
        t0 = time.time(); auc = pl.curva_evento(None, None, None, s, prep=prep).pr_auc; t_curva = time.time() - t0
        print(json.dumps({"familia": fam, "fit_s": round(t_fit, 1), "pred_val_s": round(t_pred, 1),
                          "curva_s": round(t_curva, 1), "pr_auc_evento_pliegue4": round(auc, 5)}), flush=True)
    except Exception as e:  # noqa: BLE001 - se quiere ver qué familia falla y seguir con las demás
        print(json.dumps({"familia": fam, "error": f"{type(e).__name__}: {e}"[:500]}), flush=True)
