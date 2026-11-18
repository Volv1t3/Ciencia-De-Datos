"""Protocolo común de los notebooks 03 y 04: una familia → CV temporal → MLflow → OOF en disco.

Vive en src/ (no en el notebook) para que un mismo experimento pueda correr desde el notebook o
desde un script en otra GPU (`scripts/correr_familia.py`), con exactamente la misma lógica. Cada
corrida deja `<nombre>.json` (resumen + run_id) y `<nombre>.parquet` (predicciones out-of-fold) en
`oof_dir`; si el json ya existe, no se vuelve a entrenar: el notebook se puede re-ejecutar y
retoma donde quedó.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src import datos, pipeline as pl

DESCRIPCION_CV = "EmbargoTimeSeriesSplit(n_splits={n}, meses_validacion={m}, embargo_dias={e})"


@dataclass
class Contexto:
    train: pd.DataFrame
    columnas: list[str]
    splitter: pl.EmbargoTimeSeriesSplit
    oof_dir: Path
    meta_datos: dict[str, str]
    variante: str
    frac_sanos: float = 0.15
    seed: int = pl.SEMILLA
    params_comunes: dict = field(default_factory=dict)

    @property
    def descripcion_cv(self) -> str:
        s = self.splitter
        return DESCRIPCION_CV.format(n=s.n_splits, m=s.meses_validacion, e=s.embargo_dias)


def preparar(datos_dir: str | Path, oof_dir: str | Path, *, desde=None, hasta=datos.FIN_ENTRENAMIENTO,
             splitter: pl.EmbargoTimeSeriesSplit | None = None, frac_sanos: float = 0.15,
             eda_json: str | Path = "../docs/eda_decisiones.json") -> Contexto:
    """Carga la caché del período de entrenamiento/CV y fija el protocolo común."""
    datos_dir, oof_dir = Path(datos_dir), Path(oof_dir)
    oof_dir.mkdir(parents=True, exist_ok=True)
    snapshot = datos_dir / "obt_mc1"
    manifiesto = datos.leer_manifiesto(snapshot)
    eda = json.loads(Path(eda_json).read_text())
    train = datos.cargar_cache(datos_dir / "cache_R", desde=desde, hasta=hasta,
                               sha_esperado=manifiesto["dataset_sha256"])
    return Contexto(train=train, columnas=pl.columnas_modelo(train, eda["variante"]),
                    splitter=splitter or pl.EmbargoTimeSeriesSplit(n_splits=4, meses_validacion=2, embargo_dias=30),
                    oof_dir=oof_dir, meta_datos=datos.metadatos_dataset(snapshot), variante=eda["variante"],
                    frac_sanos=frac_sanos)


def correr(ctx: Contexto, nombre: str, fase: str, familia: str, params: dict | None = None,
           modelo=None, tags: dict | None = None, params_registro: dict | None = None,
           nested: bool = False, al_terminar_pliegue=None) -> dict:
    """Evalúa con la CV común, registra en MLflow y guarda las OOF. Reutiliza si ya corrió."""
    ruta_json = ctx.oof_dir / f"{nombre}.json"
    if ruta_json.exists():
        r = json.loads(ruta_json.read_text())
        print(f"{nombre}: ya registrado (run {r['run_id'][:8]}), se reutiliza", flush=True)
        return r
    t = time.time()
    res, ultimo = pl.evaluar_cv(modelo if modelo is not None else familia, ctx.train, ctx.columnas,
                                ctx.splitter, params, seed=ctx.seed, frac_sanos_train=ctx.frac_sanos,
                                al_terminar_pliegue=al_terminar_pliegue)
    resumen = res.resumen
    metricas = resumen | {f"cv_pr_auc_evento_pliegue{i}": v for i, v in enumerate(res.cv_pliegues)}
    run_id = pl.registrar_corrida(
        nombre=nombre, fase=fase, familia=familia, modelo=ultimo, seed=ctx.seed, metricas=metricas,
        datos=ctx.meta_datos, X_ejemplo=ctx.train.iloc[:5][ctx.columnas], nested=nested,
        params_extra={"frac_sanos_train": ctx.frac_sanos, "variante": ctx.variante,
                      "n_features": len(ctx.columnas), "cv": ctx.descripcion_cv,
                      "metrica_seleccion": "pr_auc_evento (episodios de 30 dias, acierto 7-30 d)",
                      **ctx.params_comunes, **(params_registro or {})},
        tags_extra=tags)
    res.oof.to_parquet(ctx.oof_dir / f"{nombre}.parquet")
    r = {"nombre": nombre, "fase": fase, "familia": familia, "run_id": run_id,
         "cv_pliegues": res.cv_pliegues, "train_pliegues": res.train_pliegues, **resumen}
    ruta_json.write_text(json.dumps(r, indent=2))
    print(f"{nombre}: CV {resumen['cv_pr_auc_evento_media']:.4f} ± {resumen['cv_pr_auc_evento_std']:.4f} · "
          f"train {resumen['train_pr_auc_evento']:.4f} · {(time.time() - t) / 60:.1f} min · run {run_id[:8]}",
          flush=True)
    return r


def resultados(oof_dir: str | Path) -> dict[str, dict]:
    """Todas las corridas registradas en `oof_dir` (de este notebook o de scripts en otra GPU)."""
    return {p.stem: json.loads(p.read_text()) for p in sorted(Path(oof_dir).glob("*.json"))}


def gpu_visible() -> str:
    return os.getenv("CUDA_VISIBLE_DEVICES", "todas")
