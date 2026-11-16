"""Tuning de una o más familias (estudio de Optuna + corridas anidadas en MLflow) en la GPU asignada.

Uso (en el contenedor, desde pset_3/):
    python scripts/tunear.py xgboost > /datos/tuning_xgboost.log
    # en el trabajador extra (otra GPU):
    docker compose exec -d trabajador-extra sh -c "python -u scripts/tunear.py bosque > /datos/tuning_bosque.log 2>&1"

Variables: N_TRIALS (30 por defecto), FRAC_SANOS_TUNING (0.05 por defecto).
El estudio vive en /datos/optuna/pset3.db: el notebook 04 retoma lo que estos procesos dejen y
muestra los resultados.
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ / "notebooks")

from dotenv import load_dotenv  # noqa: E402

from src import experimentos as exp, pipeline as pl, tuning  # noqa: E402

load_dotenv("../.env")
DATOS = Path(os.environ.get("DATOS_DIR", "/datos"))
N_TRIALS = int(os.getenv("N_TRIALS", tuning.N_TRIALS))
FRAC = float(os.getenv("FRAC_SANOS_TUNING", "0.05"))

pl.configurar_mlflow()
ctx = exp.preparar(DATOS, DATOS / "oof" / "04", frac_sanos=FRAC)
storage = tuning.storage_por_defecto(DATOS)
print(f"GPU {exp.gpu_visible()} · {len(ctx.train):,} filas · frac_sanos {FRAC} · {N_TRIALS} trials · "
      f"commit {pl.commit_git()}", flush=True)
for familia in sys.argv[1:]:
    st = tuning.tunear(ctx, familia, storage, n_trials=N_TRIALS)
    print(f"{familia}: mejor CV {st.best_value:.4f} en el trial {st.best_trial.number} · {st.best_params}", flush=True)
