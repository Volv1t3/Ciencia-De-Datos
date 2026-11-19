"""Tuning de hiperparámetros (notebook 04): Optuna TPE + poda por mediana, corridas anidadas en MLflow.

- Un estudio de Optuna por familia, guardado en SQLite junto a los datos: si el proceso se corta,
  `tunear` retoma donde quedó (mismas configuraciones ya evaluadas, misma corrida padre).
- Cada configuración es una corrida HIJA (`nested=True`) de una corrida padre `tuning_<familia>`,
  con parámetros, semilla, datos, métricas por pliegue y el modelo, como cualquier otra corrida.
- Poda: después de cada pliegue se reporta la PR-AUC por evento acumulada; `MedianPruner` corta las
  configuraciones que van por debajo de la mediana de las anteriores en el mismo pliegue. Los
  pliegues son expansivos, así que el primero es el más barato: las malas se descartan pronto.
- Escalas: logarítmica para tasas de aprendizaje, regularización y pesos; lineal para profundidades,
  fracciones y número de épocas.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np

from src import experimentos as exp, pipeline as pl

N_TRIALS = 30
# El paso "prep" de cada pliegue se ajusta una vez y sus matrices se reutilizan en todas las
# configuraciones (pipeline.evaluar_cv). En la MLP y la logística ahorra el preprocesamiento; en los
# árboles, copiar ~70 GB por configuración desde la caché en disco (en el bosque eran ~35 de los
# ~60 min de cada configuración). Cuesta ~120-150 GB de RAM por proceso.
FAMILIAS_CON_CACHE = {"mlp", "logistica", "xgboost", "bosque"}
_CACHE_PREP: dict = {}


def _xgboost(t):
    return {"n_estimators": t.suggest_int("n_estimators", 200, 1500, log=True),
            "learning_rate": t.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "max_depth": t.suggest_int("max_depth", 3, 12),
            "min_child_weight": t.suggest_float("min_child_weight", 1.0, 200.0, log=True),
            "subsample": t.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": t.suggest_float("colsample_bytree", 0.2, 1.0),
            "reg_lambda": t.suggest_float("reg_lambda", 1e-3, 100.0, log=True),
            "reg_alpha": t.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "gamma": t.suggest_float("gamma", 0.0, 5.0)}


def _bosque(t):
    return {"n_estimators": t.suggest_int("n_estimators", 100, 600, log=True),
            "max_depth": t.suggest_int("max_depth", 6, 16),
            "subsample": t.suggest_float("subsample", 0.3, 0.9),
            "colsample_bynode": t.suggest_float("colsample_bynode", 0.02, 0.5, log=True),
            "min_child_weight": t.suggest_float("min_child_weight", 1.0, 200.0, log=True),
            "reg_lambda": t.suggest_float("reg_lambda", 1e-3, 100.0, log=True)}


def _mlp(t):
    arquitecturas = {"128-64": (128, 64), "256-128": (256, 128), "512-256": (512, 256),
                     "512-256-128": (512, 256, 128)}
    return {"ocultas": arquitecturas[t.suggest_categorical("arquitectura", list(arquitecturas))],
            "dropout": t.suggest_float("dropout", 0.0, 0.5),
            "lr": t.suggest_float("lr", 1e-4, 1e-2, log=True),
            "weight_decay": t.suggest_float("weight_decay", 1e-6, 1e-2, log=True),
            "max_epochs": t.suggest_int("max_epochs", 3, 10),
            "batch_size": t.suggest_categorical("batch_size", [16384, 32768, 65536]),
            "pos_weight": t.suggest_float("pos_weight", 5.0, 300.0, log=True)}


def _logistica(t):
    return {"C": t.suggest_float("C", 1e-4, 10.0, log=True),
            "max_iter": t.suggest_int("max_iter", 200, 2000, log=True)}


ESPACIOS = {"xgboost": _xgboost, "bosque": _bosque, "mlp": _mlp, "logistica": _logistica}


N_COMPLETAS = 30
# Protocolo final: 30 configuraciones con la CV COMPLETA por familia. La primera ronda usó poda por
# mediana y solo 18 (bosque), 15 (MLP) y 19 (XGBoost) llegaron al final. Bosque y MLP se rehicieron
# desde cero sin poda en estudios nuevos (v2), con el código ya commiteado: sus estudios originales
# tenían corridas registradas con commit "-dirty". XGBoost conserva su estudio (limpio) y se extendió
# sin poda hasta tener 30 completas; sus 11 configuraciones podadas siguen registradas.
ESTUDIO = {"bosque": "pset3-bosque-v2", "mlp": "pset3-mlp-v2"}


def nombre_estudio(familia: str) -> str:
    return ESTUDIO.get(familia, f"pset3-{familia}")


def estudio(familia: str, storage: str, seed: int = pl.SEMILLA, podar: bool = False):
    """Estudio de Optuna (TPE). El pruner no se guarda en el storage: aplica a las configuraciones
    que se evalúen desde esta llamada. `podar=False`: toda configuración nueva completa la CV."""
    import optuna  # noqa: PLC0415
    pruner = (optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=0) if podar
              else optuna.pruners.NopPruner())
    return optuna.create_study(
        study_name=nombre_estudio(familia), storage=storage, load_if_exists=True, direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed, n_startup_trials=8), pruner=pruner)


def _contar(st, estado) -> int:
    return sum(t.state == estado for t in st.trials)


def tunear(ctx: exp.Contexto, familia: str, storage: str, completas: int = N_COMPLETAS,
           podar: bool = False) -> "optuna.Study":
    """Corre (o retoma) el estudio de `familia` hasta tener `completas` configuraciones con CV completa."""
    import mlflow  # noqa: PLC0415
    import optuna  # noqa: PLC0415

    if familia not in ESPACIOS:
        raise ValueError(f"Sin espacio de búsqueda para {familia}")
    st = estudio(familia, storage, ctx.seed, podar)
    COMPLETA = optuna.trial.TrialState.COMPLETE
    padre = st.user_attrs.get("mlflow_run_padre")
    nombre_padre = "tuning_" + nombre_estudio(familia).removeprefix("pset3-")
    with mlflow.start_run(run_id=padre) if padre else mlflow.start_run(run_name=nombre_padre) as run:
        if not padre:
            st.set_user_attr("mlflow_run_padre", run.info.run_id)
            mlflow.set_tags({"fase": "tuning", "familia": familia, "git_commit": pl.commit_git(),
                             "rol": "padre", "estudio": nombre_estudio(familia)})
            mlflow.log_params({"semilla": ctx.seed, "configuraciones_completas_objetivo": completas,
                               "sampler": "TPE(n_startup_trials=8)",
                               "pruner": "MedianPruner(n_startup_trials=5)" if podar else "ninguno",
                               "frac_sanos_train": ctx.frac_sanos, "cv": ctx.descripcion_cv, **ctx.meta_datos})
        for _ in range(3):  # tope de rondas: si alguna configuración falla, se reintenta pocas veces
            faltan = completas - _contar(st, COMPLETA)
            if faltan <= 0:
                break
            st.optimize(lambda t: _objetivo(t, ctx, familia), n_trials=faltan, gc_after_trial=True)
        mejor = st.best_trial
        mlflow.log_metrics({"mejor_cv_pr_auc_evento": mejor.value,
                            "trials_completos": _contar(st, COMPLETA),
                            "trials_podados": _contar(st, optuna.trial.TrialState.PRUNED)})
        # Tags (no params): el mejor puede cambiar al extender un estudio, y un param no se puede reescribir
        mlflow.set_tags({f"mejor_{k}": str(v) for k, v in mejor.params.items()}
                        | {"mejor_trial": str(mejor.number), "configuraciones_completas": str(_contar(st, COMPLETA))})
    return st


def _objetivo(trial, ctx: exp.Contexto, familia: str) -> float:
    import optuna  # noqa: PLC0415

    params = ESPACIOS[familia](trial)
    acumulado: list[float] = []

    def reportar(i, media_cv):
        acumulado.append(media_cv)
        trial.report(media_cv, step=i)
        if trial.should_prune():
            raise optuna.TrialPruned()

    nombre = f"{familia}_trial{trial.number:02d}"
    try:
        res, ultimo = pl.evaluar_cv(familia, ctx.train, ctx.columnas, ctx.splitter, dict(params),
                                    seed=ctx.seed, frac_sanos_train=ctx.frac_sanos,
                                    al_terminar_pliegue=reportar,
                                    cache_prep=_CACHE_PREP if familia in FAMILIAS_CON_CACHE else None)
    except optuna.TrialPruned:
        _registrar_podado(ctx, familia, nombre, trial, params, acumulado)
        raise
    resumen = res.resumen
    pl.registrar_corrida(
        nombre=nombre, fase="tuning", familia=familia, modelo=ultimo, seed=ctx.seed, nested=True,
        metricas=resumen | {f"cv_pr_auc_evento_pliegue{i}": v for i, v in enumerate(res.cv_pliegues)},
        datos=ctx.meta_datos, X_ejemplo=ctx.train.iloc[:5][ctx.columnas],
        params_extra={"trial": trial.number, "frac_sanos_train": ctx.frac_sanos, "cv": ctx.descripcion_cv,
                      **{f"hp_{k}": v for k, v in trial.params.items()}},
        tags_extra={"estado_trial": "completo"})
    for k in ("train_pr_auc_evento", "cv_pr_auc_evento_std", "brecha_train_cv", "segundos_entrenamiento"):
        trial.set_user_attr(k, float(resumen[k]))
    print(f"{nombre}: CV {resumen['cv_pr_auc_evento_media']:.4f} ± {resumen['cv_pr_auc_evento_std']:.4f} "
          f"({resumen['segundos_entrenamiento'] / 60:.1f} min de fit)", flush=True)
    return float(resumen["cv_pr_auc_evento_media"])


def _registrar_podado(ctx, familia, nombre, trial, params, acumulado):
    """Una configuración podada también queda en MLflow (sin modelo: no terminó la CV)."""
    import mlflow  # noqa: PLC0415
    with mlflow.start_run(run_name=nombre, nested=True):
        mlflow.log_params({"semilla": ctx.seed, "trial": trial.number, "frac_sanos_train": ctx.frac_sanos,
                           "cv": ctx.descripcion_cv, **{f"hp_{k}": v for k, v in trial.params.items()},
                           **ctx.meta_datos})
        mlflow.log_metrics({f"cv_pr_auc_evento_acumulado_pliegue{i}": v for i, v in enumerate(acumulado)})
        mlflow.set_tags({"fase": "tuning", "familia": familia, "git_commit": pl.commit_git(),
                         "estado_trial": f"podado tras el pliegue {len(acumulado) - 1}"})
    print(f"{nombre}: podado tras el pliegue {len(acumulado) - 1} "
          f"(CV acumulada {acumulado[-1]:.4f})", flush=True)


def storage_por_defecto(datos_dir: str | Path) -> str:
    """SQLite junto a los datos (un archivo por proyecto; cada familia es un estudio distinto)."""
    ruta = Path(datos_dir) / "optuna" / "pset3.db"
    ruta.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{ruta}"


def resumen_estudio(st) -> "pd.DataFrame":
    import pandas as pd  # noqa: PLC0415
    df = st.trials_dataframe(attrs=("number", "value", "state", "params", "user_attrs", "duration"))
    return df.sort_values("value", ascending=False, na_position="last")
