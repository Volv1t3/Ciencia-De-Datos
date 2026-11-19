"""Marca las corridas `fase=final` que ya no son las vigentes y vuelve a exportar docs/corridas.csv.

Uso (en el contenedor, desde pset_3/):
    python scripts/marcar_finales_reemplazadas.py

El notebook 05 se ejecutó dos veces (ver sus conclusiones), así que hay dos corridas finales por rol.
La vigente de cada rol es la que apunta el alias del Model Registry (`@champion`, `@challenger`); las
demás reciben los tags `estado=reemplazada` y `reemplazada_por=<run_id vigente>`. No borra nada, no
entrena y no toca la prueba: solo agrega tags y reescribe el CSV. Se puede correr varias veces.
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ / "notebooks")

from mlflow import MlflowClient  # noqa: E402

from src import pipeline as pl  # noqa: E402

mlflow = pl.configurar_mlflow()
cliente = MlflowClient()
finales = mlflow.search_runs(experiment_names=[pl.NOMBRE_EXPERIMENTO], filter_string="tags.fase = 'final'")

for rol in ("champion", "challenger"):
    vigente = cliente.get_model_version_by_alias(pl.NOMBRE_MODELO_REGISTRO, rol).run_id
    cliente.set_tag(vigente, "estado", "vigente")
    for run_id in finales.loc[finales["tags.rol"] == rol, "run_id"]:
        if run_id != vigente:
            cliente.set_tag(run_id, "estado", "reemplazada")
            cliente.set_tag(run_id, "reemplazada_por", vigente)
            print(f"{rol}: {run_id} -> reemplazada por {vigente}")

corridas = pl.exportar_corridas("../docs/corridas.csv")
print(f"docs/corridas.csv: {len(corridas)} corridas")
print(corridas.loc[corridas["tags.fase"] == "final", ["run_id", "tags.rol", "tags.estado"]].to_string(index=False))
