"""Marca en MLflow las corridas que ya no son vigentes y vuelve a exportar docs/corridas.csv.

Uso (en el contenedor, desde pset_3/):
    python scripts/marcar_finales_reemplazadas.py

No borra nada, no entrena y no toca la prueba: solo agrega tags y reescribe el CSV. Se puede correr
varias veces. Reglas:
- `fase=final`: la vigente de cada rol es la que apunta el alias del Model Registry (`@champion`,
  `@challenger`); las demás de ese rol quedan `estado=reemplazada`.
- Corridas repetidas con el mismo nombre (`<familia>_optimizado`, `tabpfn`, `llm_gemini`): la más
  reciente es la vigente.
- Tuning: los estudios originales del bosque y la MLP (padres `tuning_bosque` y `tuning_mlp`, con poda y
  corridas "-dirty") fueron reemplazados por `tuning_bosque-v2` y `tuning_mlp-v2` (sin poda, 30
  configuraciones completas). El padre y todas sus configuraciones quedan `estado=reemplazada`.
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
todas = mlflow.search_runs(experiment_names=[pl.NOMBRE_EXPERIMENTO], max_results=50_000)
nombre = todas["tags.mlflow.runName"].astype(str)


def marcar(run_id: str, por: str, motivo: str) -> None:
    cliente.set_tag(run_id, "estado", "reemplazada")
    cliente.set_tag(run_id, "reemplazada_por", por)
    cliente.set_tag(run_id, "motivo_reemplazo", motivo)


# 1) Finales: la vigente es la del alias del registry
finales = todas[todas["tags.fase"] == "final"]
for rol in ("champion", "challenger"):
    vigente = cliente.get_model_version_by_alias(pl.NOMBRE_MODELO_REGISTRO, rol).run_id
    cliente.set_tag(vigente, "estado", "vigente")
    for run_id in finales.loc[finales["tags.rol"] == rol, "run_id"]:
        if run_id != vigente:
            marcar(run_id, vigente, "evaluación final anterior")
            print(f"final {rol}: {run_id} -> {vigente}")

# 2) Corridas repetidas con el mismo nombre: vigente la más reciente
repetibles = [n for n in nombre.unique() if n.endswith("_optimizado") or n in ("tabpfn", "llm_gemini")]
for n in repetibles:
    grupo = todas[nombre == n].sort_values("start_time", ascending=False)
    if len(grupo) == 0:
        continue
    vigente = grupo["run_id"].iloc[0]
    cliente.set_tag(vigente, "estado", "vigente")
    for run_id in grupo["run_id"].iloc[1:]:
        marcar(run_id, vigente, "re-ejecución posterior")
        print(f"{n}: {run_id} -> {vigente}")

# 3) Estudios de tuning originales del bosque y la MLP (con poda y corridas "-dirty")
for familia in ("bosque", "mlp"):
    nuevos = todas[nombre == f"tuning_{familia}-v2"]
    viejos = todas[nombre == f"tuning_{familia}"]
    if len(nuevos) == 0 or len(viejos) == 0:
        continue
    padre_nuevo = nuevos["run_id"].iloc[0]
    for padre_viejo in viejos["run_id"]:
        hijas = todas.loc[todas.get("tags.mlflow.parentRunId") == padre_viejo, "run_id"]
        for run_id in [padre_viejo, *hijas]:
            marcar(run_id, padre_nuevo, "estudio sin poda (30 configuraciones completas) y commit limpio")
        print(f"tuning {familia}: padre {padre_viejo} y {len(hijas)} configuraciones -> {padre_nuevo}")

corridas = pl.exportar_corridas("../docs/corridas.csv")
estado = corridas.get("tags.estado")
vigentes = corridas[estado != "reemplazada"] if estado is not None else corridas
sucias = vigentes["tags.git_commit"].astype(str).str.endswith("-dirty").sum()
print(f"docs/corridas.csv: {len(corridas)} corridas ({len(corridas) - len(vigentes)} reemplazadas, "
      f"{len(vigentes)} vigentes; vigentes con commit '-dirty': {sucias})")
