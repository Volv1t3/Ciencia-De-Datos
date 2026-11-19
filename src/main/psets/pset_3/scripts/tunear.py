"""Tuning de una o más familias (estudio de Optuna + corridas anidadas en MLflow) en la GPU asignada.

Uso (en el contenedor, desde pset_3/):
    python scripts/tunear.py xgboost > /datos/tuning_xgboost.log
    # en el trabajador extra (otra GPU):
    docker compose exec -d trabajador-extra sh -c "python -u scripts/tunear.py bosque > /datos/tuning_bosque.log 2>&1"

Variables:
    N_COMPLETAS (30 configuraciones con CV completa) · FRAC_SANOS_TUNING (0.05) · PODAR (0)
    Solo para pruebas reducidas: PRUEBA_DESDE (AAAA-MM-DD), PRUEBA_PLIEGUES, PRUEBA_MESES_VAL; con
    PRUEBA_DESDE se usa el experimento "pset3-pruebas" y una base de Optuna y OOF aparte.
El estudio real vive en /datos/optuna/pset3.db: el notebook 04 retoma lo que estos procesos dejen.
"""
import os
import sys
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ / "notebooks")

from dotenv import load_dotenv  # noqa: E402

from src import experimentos as exp, pipeline as pl, tuning  # noqa: E402

load_dotenv("../.env")
DATOS = Path(os.environ.get("DATOS_DIR", "/datos"))
N_COMPLETAS = int(os.getenv("N_COMPLETAS", tuning.N_COMPLETAS))
PODAR = os.getenv("PODAR", "0") == "1"
FRAC = float(os.getenv("FRAC_SANOS_TUNING", "0.05"))
PRUEBA = os.getenv("PRUEBA_DESDE")

if PRUEBA:
    pl.configurar_mlflow("pset3-pruebas")
    splitter = pl.EmbargoTimeSeriesSplit(n_splits=int(os.getenv("PRUEBA_PLIEGUES", "2")),
                                         meses_validacion=int(os.getenv("PRUEBA_MESES_VAL", "1")))
    ctx = exp.preparar(DATOS, DATOS / "oof" / "04_prueba", desde=date.fromisoformat(PRUEBA),
                       splitter=splitter, frac_sanos=FRAC)
    (DATOS / "optuna").mkdir(parents=True, exist_ok=True)
    storage = f"sqlite:///{DATOS / 'optuna' / 'prueba.db'}"
else:
    pl.configurar_mlflow()
    ctx = exp.preparar(DATOS, DATOS / "oof" / "04", frac_sanos=FRAC)
    storage = tuning.storage_por_defecto(DATOS)
print(f"GPU {exp.gpu_visible()} · {len(ctx.train):,} filas · frac_sanos {FRAC} · {N_COMPLETAS} completas · poda {PODAR} · "
      f"{'PRUEBA' if PRUEBA else 'real'} · commit {pl.commit_git()}", flush=True)
for familia in sys.argv[1:]:
    st = tuning.tunear(ctx, familia, storage, completas=N_COMPLETAS, podar=PODAR)
    print(f"{familia}: mejor CV {st.best_value:.4f} en el trial {st.best_trial.number} · {st.best_params}", flush=True)
