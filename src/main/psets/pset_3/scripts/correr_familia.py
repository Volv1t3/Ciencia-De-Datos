"""Corre una o más familias del notebook 03 con el mismo protocolo, en la GPU que se le asigne.

Uso (en el contenedor, desde pset_3/):
    CUDA_VISIBLE_DEVICES=1 python scripts/correr_familia.py tabpfn > /datos/familia_tabpfn.log

Escribe en el mismo directorio de OOF que el notebook 03; al re-ejecutar el notebook, estas
corridas aparecen en la comparación y no se vuelven a entrenar.
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ / "notebooks")  # mismas rutas relativas que los notebooks (../docs, ../.env)

from dotenv import load_dotenv  # noqa: E402

from src import experimentos as exp, pipeline as pl  # noqa: E402

load_dotenv("../.env")
DATOS = Path(os.environ.get("DATOS_DIR", "/datos"))
pl.configurar_mlflow()
ctx = exp.preparar(DATOS, DATOS / "oof" / "03")
print(f"GPU {exp.gpu_visible()} · {len(ctx.train):,} filas · {len(ctx.columnas)} features · commit {pl.commit_git()}",
      flush=True)
for familia in sys.argv[1:]:
    exp.correr(ctx, familia, "candidato", familia)
