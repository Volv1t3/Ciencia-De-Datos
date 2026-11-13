"""Preparación, modelos, validación temporal, métricas por evento y registro en MLflow.

Los notebooks importan este módulo; no copian su lógica.

Definiciones comprometidas en el PSet #1 (memo Perla_Guaman_Teran_Arellano, sección 3.1):
- Un *episodio de alerta* por disco: la primera fecha con score >= tau.
- Acierto: el disco falla entre 7 y 30 días después de esa fecha (DAYS_TO_FAILURE en [7, 30]).
- Recall sobre eventos (discos que fallan), nunca sobre filas.
- Selección por PR-AUC por evento; operación por F0,5 por evento.
- Negocio: costo esperado por 1.000 SSD-mes, C_FP = 1 y C_FN = k, para k en [1, 50].
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin, TransformerMixin, clone
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

SEMILLA = 42

# --------------------------------------------------------------------------------------------
# Columnas
# --------------------------------------------------------------------------------------------
COLUMNAS_META = ("SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D")
# Fuga si entran como feature: describen el futuro del disco o son identificadores.
COLUMNAS_PROHIBIDAS = frozenset({
    "FIRST_FAILURE_DATE", "LAST_SEEN_DATE", "DAYS_TO_FAILURE", "LABEL_STATUS", "TARGET_30D",
    "SMART_DAILY_KEY", "SSD_KEY", "DISK_ID", "MODEL_CODE", "OBSERVATION_DATE_KEY", "OBSERVATION_DATE",
})

PATRON_VALOR_DIA = r"^[RN]_\d+$"  # valor del día, sin ventanas: baseline "sin features temporales"
# Atributos SMART de error/desgaste presentes en MC1. La EDA (01_eda) confirma cuáles separan.
ATRIBUTOS_CRITICOS_DEFECTO = (5, 171, 172, 183, 184, 187, 196, 197, 198, 199)
FEATURE_DEGRADACION = "N_CRITICAL_DEGRADING_30D"


def columnas_modelo(df: pd.DataFrame, variante: str = "R") -> list[str]:
    """Features que recibe el modelo: SMART de la variante (R, N o RN) + derivadas del SQL."""
    if variante not in {"R", "N", "RN"}:
        raise ValueError("variante debe ser R, N o RN")
    prefijos = tuple(f"{v}_" for v in variante)  # "RN" -> ("R_", "N_")
    cols = [c for c in df.columns
            if c not in COLUMNAS_PROHIBIDAS and (c.startswith(prefijos) or c == "DAYS_SINCE_FIRST_SEEN")]
    verificar_sin_fuga(cols)
    return cols


def verificar_sin_fuga(columnas) -> None:
    fuga = COLUMNAS_PROHIBIDAS.intersection(columnas)
    if fuga:
        raise AssertionError(f"Columnas con fuga en X: {sorted(fuga)}")


# --------------------------------------------------------------------------------------------
# Features derivadas dentro del pipeline
# --------------------------------------------------------------------------------------------
class FeaturesDerivadas(BaseEstimator, TransformerMixin):
    """Agrega N_CRITICAL_DEGRADING_30D: cuántos atributos críticos empeoran en 30 días.

    Usa solo `{R|N}_{id}_MEAN_SHIFT_30D`, que compara la media de los últimos 30 días con la de
    los 30 anteriores: es información disponible el día de la predicción. Sin estado aprendido.
    """

    def __init__(self, atributos=ATRIBUTOS_CRITICOS_DEFECTO, representacion: str = "R"):
        self.atributos = atributos
        self.representacion = representacion

    def _columnas(self, X):
        return [c for a in self.atributos
                if (c := f"{self.representacion}_{a}_MEAN_SHIFT_30D") in X.columns]

    def fit(self, X, y=None):
        self.columnas_shift_ = self._columnas(X)
        self.feature_names_in_ = np.asarray(X.columns, dtype=object)
        return self

    def transform(self, X):
        X = X.copy()
        shifts = X[self.columnas_shift_].to_numpy(dtype=np.float32, na_value=np.nan)
        X[FEATURE_DEGRADACION] = np.nansum(shifts > 0, axis=1).astype(np.float32)
        return X

    def get_feature_names_out(self, input_features=None):
        return np.append(self.feature_names_in_, FEATURE_DEGRADACION)


def _log1p_con_signo(x):
    """log1p simétrico: comprime colas largas y respeta tendencias negativas (MEAN_SHIFT)."""
    return np.sign(x) * np.log1p(np.abs(x))


def _a_float32(a):
    """skorch/torch esperan float32."""
    return np.asarray(a, dtype=np.float32)


def construir_preprocesador(familia: str) -> Pipeline:
    """Preprocesamiento por familia; se ajusta siempre solo con datos de entrenamiento.

    - Árboles (bosque, xgboost, regla): reciben NaN de forma nativa; no se imputa ni escala.
    - Lineales, MLP y TabPFN: log1p con signo -> mediana + indicador de faltante -> estándar.
      La imputación vive aquí (no en Silver ni en la OBT) para ajustarse solo con train.
    """
    pasos = [("derivadas", FeaturesDerivadas())]
    if familia in {"logistica", "sin_temporales", "mlp", "tabpfn"}:
        numericas = Pipeline([
            ("log1p", FunctionTransformer(_log1p_con_signo, feature_names_out="one-to-one")),
            ("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=False)),
            ("escalar", StandardScaler()),
        ])
        selector = (make_column_selector(pattern=PATRON_VALOR_DIA)
                    if familia == "sin_temporales" else make_column_selector(dtype_include=np.number))
        pasos.append(("columnas", ColumnTransformer([("num", numericas, selector)],
                                                   remainder="drop", verbose_feature_names_out=False)))
        pasos.append(("float32", FunctionTransformer(_a_float32)))
    return Pipeline(pasos)


# --------------------------------------------------------------------------------------------
# Estimadores propios
# --------------------------------------------------------------------------------------------
class ReglaUmbralSMART(BaseEstimator, ClassifierMixin):
    """Baseline de negocio: regla de umbral sobre un solo atributo SMART (PSet #1).

    El score es la función de distribución empírica del atributo en entrenamiento, así que es
    monótono en el atributo y vive en [0, 1]: barrer tau equivale a barrer el umbral sobre el
    atributo. Faltantes -> 0 (sin evidencia, no alerta).
    """

    def __init__(self, atributo: str = "R_5"):
        self.atributo = atributo

    def fit(self, X, y):
        valores = pd.to_numeric(X[self.atributo], errors="coerce").to_numpy(dtype=np.float64)
        self.referencia_ = np.sort(valores[~np.isnan(valores)])
        self.classes_ = np.array([0, 1])
        return self

    def predict_proba(self, X):
        v = pd.to_numeric(X[self.atributo], errors="coerce").to_numpy(dtype=np.float64)
        p = np.searchsorted(self.referencia_, np.nan_to_num(v, nan=-np.inf), side="right")
        p = p / max(len(self.referencia_), 1)
        return np.column_stack([1 - p, p])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


class ConSubmuestra(BaseEstimator, ClassifierMixin):
    """Entrena `estimador` con una submuestra estratificada de a lo sumo `n_max` filas.

    TabPFN recibe el entrenamiento como contexto y no admite más de ~50k filas con cualquier
    hardware: es una limitación del modelo, no del cómputo, y se documenta como tal.
    """

    def __init__(self, estimador=None, n_max: int = 50_000, random_state: int = SEMILLA):
        self.estimador = estimador
        self.n_max = n_max
        self.random_state = random_state

    def fit(self, X, y):
        y = np.asarray(y)
        rng = np.random.default_rng(self.random_state)
        if len(y) > self.n_max:
            pos, neg = np.flatnonzero(y == 1), np.flatnonzero(y == 0)
            n_pos = min(len(pos), self.n_max // 2)
            idx = np.concatenate([rng.choice(pos, n_pos, replace=False),
                                  rng.choice(neg, self.n_max - n_pos, replace=False)])
            X, y = (X.iloc[idx] if hasattr(X, "iloc") else X[idx]), y[idx]
        self.estimador_ = clone(self.estimador).fit(X, y)
        self.classes_ = self.estimador_.classes_
        return self

    def predict_proba(self, X):
        return self.estimador_.predict_proba(X)

    def predict(self, X):
        return self.estimador_.predict(X)


def _mlp_skorch(params: dict, seed: int):
    import torch  # noqa: PLC0415
    from skorch import NeuralNetBinaryClassifier  # noqa: PLC0415

    class MLP(torch.nn.Module):
        def __init__(self, n_entrada=-1, ocultas=(256, 128), dropout=0.1):
            super().__init__()
            capas, previo = [], n_entrada
            for h in ocultas:
                capas += [torch.nn.LazyLinear(h) if previo < 0 else torch.nn.Linear(previo, h),
                          torch.nn.ReLU(), torch.nn.Dropout(dropout)]
                previo = h
            capas.append(torch.nn.Linear(previo, 1))
            self.red = torch.nn.Sequential(*capas)

        def forward(self, X):
            return self.red(X).squeeze(-1)

    torch.manual_seed(seed)
    p = {"ocultas": (256, 128), "dropout": 0.1, "lr": 1e-3, "weight_decay": 1e-5,
         "max_epochs": 10, "batch_size": 8192, "pos_weight": 50.0} | params
    return NeuralNetBinaryClassifier(
        MLP, module__ocultas=tuple(p["ocultas"]), module__dropout=p["dropout"],
        criterion=torch.nn.BCEWithLogitsLoss,
        criterion__pos_weight=torch.tensor(p["pos_weight"]),
        optimizer=torch.optim.AdamW, optimizer__weight_decay=p["weight_decay"],
        lr=p["lr"], max_epochs=p["max_epochs"], batch_size=p["batch_size"],
        iterator_train__shuffle=True, train_split=None, verbose=0,
        device="cuda" if torch.cuda.is_available() else "cpu",
    )


def construir_estimador(familia: str, params: dict | None = None, seed: int = SEMILLA):
    """Estimador sin preprocesamiento. Imports perezosos: cada familia trae su dependencia."""
    params = dict(params or {})
    if familia == "dummy":
        from sklearn.dummy import DummyClassifier  # noqa: PLC0415
        return DummyClassifier(strategy="prior", **params)
    if familia == "regla":
        return ReglaUmbralSMART(**params)
    if familia in {"logistica", "sin_temporales"}:
        from sklearn.linear_model import LogisticRegression  # noqa: PLC0415
        return LogisticRegression(**({"max_iter": 1000, "class_weight": "balanced",
                                      "random_state": seed} | params))
    if familia == "bosque":
        from sklearn.ensemble import RandomForestClassifier  # noqa: PLC0415
        return RandomForestClassifier(**({"random_state": seed, "n_jobs": -1,
                                          "class_weight": "balanced_subsample"} | params))
    if familia == "xgboost":
        from xgboost import XGBClassifier  # noqa: PLC0415
        return XGBClassifier(**({"device": "cuda", "tree_method": "hist", "random_state": seed,
                                 "eval_metric": "aucpr", "n_jobs": -1} | params))
    if familia == "mlp":
        return _mlp_skorch(params, seed)
    if familia == "tabpfn":
        from tabpfn import TabPFNClassifier  # noqa: PLC0415
        n_max = params.pop("n_max", 50_000)
        return ConSubmuestra(TabPFNClassifier(**({"device": "cuda", "random_state": seed} | params)),
                             n_max=n_max, random_state=seed)
    raise ValueError(f"Familia desconocida: {familia}")


def construir_modelo(familia: str, params: dict | None = None, seed: int = SEMILLA) -> Pipeline:
    """Pipeline completo (preprocesamiento + modelo): es lo que se registra en MLflow."""
    return Pipeline([("prep", construir_preprocesador(familia)),
                     ("modelo", construir_estimador(familia, params, seed))])


# --------------------------------------------------------------------------------------------
# Validación cruzada temporal con embargo
# --------------------------------------------------------------------------------------------
class EmbargoTimeSeriesSplit:
    """CV de ventana expansiva por FECHAS con embargo.

    Los últimos `n_splits * meses_validacion` meses se dividen en bloques de validación
    consecutivos; cada pliegue entrena con todo lo anterior a `inicio_validacion - embargo`.

    `TimeSeriesSplit(gap=...)` cuenta filas, no días, y aquí las filas por día varían: por eso
    el splitter propio. El embargo de 30 días evita que una fila de entrenamiento en t tenga una
    etiqueta (que mira hasta t+30) solapada con el bloque de validación.
    """

    def __init__(self, n_splits: int = 4, meses_validacion: int = 2, embargo_dias: int = 30):
        self.n_splits = n_splits
        self.meses_validacion = meses_validacion
        self.embargo_dias = embargo_dias

    def bloques(self, fechas: pd.Series) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
        """[(fin_entrenamiento_exclusivo, inicio_val, fin_val_exclusivo), ...]."""
        fechas = pd.to_datetime(fechas)
        fin = fechas.max().normalize() + pd.offsets.MonthBegin(1)  # 1.er día del mes siguiente
        salida = []
        for i in range(self.n_splits, 0, -1):
            inicio_val = fin - pd.DateOffset(months=i * self.meses_validacion)
            fin_val = inicio_val + pd.DateOffset(months=self.meses_validacion)
            salida.append((inicio_val - pd.Timedelta(days=self.embargo_dias), inicio_val, fin_val))
        if salida[0][0] <= fechas.min():
            raise ValueError("No queda entrenamiento para el primer pliegue: reduce n_splits")
        return salida

    def split(self, X, y=None, groups=None) -> Iterator[tuple[np.ndarray, np.ndarray]]:
        fechas = pd.to_datetime(groups if groups is not None else X["OBSERVATION_DATE"])
        f = fechas.to_numpy()
        for fin_train, ini_val, fin_val in self.bloques(fechas):
            idx_train = np.flatnonzero(f < np.datetime64(fin_train))
            idx_val = np.flatnonzero((f >= np.datetime64(ini_val)) & (f < np.datetime64(fin_val)))
            yield idx_train, idx_val

    def get_n_splits(self, X=None, y=None, groups=None) -> int:
        return self.n_splits


# --------------------------------------------------------------------------------------------
# Métricas por evento (PSet #1)
# --------------------------------------------------------------------------------------------
ANTICIPACION_MIN, ANTICIPACION_MAX = 7, 30


def _registros_por_disco(ssd, fecha, dias, score) -> pd.DataFrame:
    """Filas donde el máximo acumulado del score de cada disco sube (orden por fecha).

    Para un umbral tau, la alerta de un disco es la primera fila con score >= tau; esa fila es
    siempre uno de estos "récords": el k-ésimo récord es la alerta para tau en (s_{k-1}, s_k].
    Así la curva PR por evento se calcula de forma exacta en O(n log n).
    """
    d = pd.DataFrame({"ssd": np.asarray(ssd), "fecha": pd.to_datetime(np.asarray(fecha)),
                      "dias": np.asarray(dias, dtype=float), "score": np.asarray(score, dtype=float)})
    d = d.sort_values(["ssd", "fecha"], kind="stable")
    maximo = d.groupby("ssd", sort=False)["score"].cummax()
    previo = maximo.groupby(d["ssd"], sort=False).shift(1)
    rec = d[previo.isna() | (d["score"] > previo)].copy()
    rec["previo"] = previo[rec.index].fillna(-np.inf)
    rec["acierto"] = rec["dias"].between(ANTICIPACION_MIN, ANTICIPACION_MAX)
    return rec


def eventos_anticipables(ssd, dias) -> int:
    """Discos con al menos una observación entre 7 y 30 días antes de su falla (denominador)."""
    d = pd.DataFrame({"ssd": np.asarray(ssd), "dias": np.asarray(dias, dtype=float)})
    return int(d.loc[d["dias"].between(ANTICIPACION_MIN, ANTICIPACION_MAX), "ssd"].nunique())


@dataclass
class CurvaEvento:
    umbrales: np.ndarray  # descendentes
    alertas: np.ndarray
    aciertos: np.ndarray
    eventos: int
    pr_auc: float = field(init=False)

    def __post_init__(self):
        """PR-AUC con precisión interpolada (estilo PASCAL VOC).

        Con la regla "primera fecha con score >= tau", el recall por evento NO es monótono en
        tau: al bajar el umbral la alerta de un disco puede adelantarse a más de 30 días de su
        falla y dejar de ser acierto. Por eso no se usa la suma de Riemann de sklearn (daría
        incrementos negativos): se ordenan los puntos por recall y se usa, en cada nivel, la
        mejor precisión alcanzable con un recall igual o mayor.
        """
        if not self.eventos or len(self.umbrales) == 0:
            self.pr_auc = float("nan")
            return
        r, p = self.recall, self.precision
        orden = np.argsort(r, kind="stable")
        r, p = np.concatenate([[0.0], r[orden]]), np.concatenate([[p[orden][0] if len(p) else 0.0], p[orden]])
        p_interp = np.maximum.accumulate(p[::-1])[::-1]
        self.pr_auc = float(np.sum(np.diff(r) * p_interp[1:]))

    @property
    def precision(self):
        return np.divide(self.aciertos, self.alertas, out=np.zeros(len(self.alertas)), where=self.alertas > 0)

    @property
    def recall(self):
        return self.aciertos / self.eventos if self.eventos else np.zeros(len(self.aciertos))


def curva_evento(ssd, fecha, dias, score) -> CurvaEvento:
    """Curva precisión-recall por evento, exacta en todos los umbrales relevantes."""
    rec = _registros_por_disco(ssd, fecha, dias, score)
    umbrales = np.unique(rec["score"].to_numpy())[::-1]
    s = np.sort(rec["score"].to_numpy())
    p = np.sort(rec["previo"].to_numpy())
    s_ac = np.sort(rec.loc[rec["acierto"], "score"].to_numpy())
    p_ac = np.sort(rec.loc[rec["acierto"], "previo"].to_numpy())

    def contar_mayor_igual(arr, t):
        return len(arr) - np.searchsorted(arr, t, side="left")

    # Récords activos en t: s_k >= t y previo_k < t  ==  #(s>=t) - #(previo>=t)
    alertas = contar_mayor_igual(s, umbrales) - contar_mayor_igual(p, umbrales)
    aciertos = contar_mayor_igual(s_ac, umbrales) - contar_mayor_igual(p_ac, umbrales)
    return CurvaEvento(umbrales, alertas, aciertos, eventos_anticipables(ssd, dias))


def metricas_evento(ssd, fecha, dias, score, tau: float, beta: float = 0.5) -> dict:
    """Precisión, recall y F-beta por evento en un umbral tau (operación)."""
    rec = _registros_por_disco(ssd, fecha, dias, score)
    alerta = rec[(rec["score"] >= tau) & (rec["previo"] < tau)]
    tp, n_alertas = int(alerta["acierto"].sum()), len(alerta)
    eventos = eventos_anticipables(ssd, dias)
    precision = tp / n_alertas if n_alertas else 0.0
    recall = tp / eventos if eventos else 0.0
    b2 = beta ** 2
    f = (1 + b2) * precision * recall / (b2 * precision + recall) if precision + recall else 0.0
    return {"precision_evento": precision, "recall_evento": recall, "f05_evento": f,
            "alertas": n_alertas, "aciertos": tp, "eventos": eventos,
            "alertas_tardias": int(alerta["dias"].between(1, ANTICIPACION_MIN - 1).sum())}


# --------------------------------------------------------------------------------------------
# Métrica de negocio: costo esperado por 1.000 SSD-mes
# --------------------------------------------------------------------------------------------
def ssd_meses(ssd, fecha) -> float:
    """Exposición observada: días-disco / 30."""
    return len(pd.DataFrame({"s": np.asarray(ssd), "f": np.asarray(fecha)}).drop_duplicates()) / 30.0


def costo_por_1000(alertas, aciertos, eventos, exposicion_ssd_mes: float, k: float):
    """C_FP = 1 por alerta que no anticipa una falla (incluye tardías); C_FN = k por falla perdida."""
    alertas, aciertos = np.asarray(alertas, float), np.asarray(aciertos, float)
    fp, fn = alertas - aciertos, eventos - aciertos
    return 1000.0 * (fp + k * fn) / exposicion_ssd_mes


def barrer_k(curva: CurvaEvento, exposicion_ssd_mes: float, ks=range(1, 51)) -> pd.DataFrame:
    """Para cada k: umbral de costo mínimo en `curva` (calcularla con predicciones OOF de CV)."""
    filas = []
    for k in ks:
        c = costo_por_1000(curva.alertas, curva.aciertos, curva.eventos, exposicion_ssd_mes, k)
        sin_modelo = 1000.0 * k * curva.eventos / exposicion_ssd_mes
        i = int(np.argmin(c))
        if c[i] >= sin_modelo:  # alertar no conviene: tau = +inf
            filas.append({"k": k, "tau": np.inf, "costo": sin_modelo, "alertas": 0, "aciertos": 0,
                          "costo_sin_modelo": sin_modelo})
        else:
            filas.append({"k": k, "tau": float(curva.umbrales[i]), "costo": float(c[i]),
                          "alertas": int(curva.alertas[i]), "aciertos": int(curva.aciertos[i]),
                          "costo_sin_modelo": sin_modelo})
    return pd.DataFrame(filas)


def costo_con_tau(ssd, fecha, dias, score, taus_por_k: pd.DataFrame) -> pd.DataFrame:
    """Aplica en otro conjunto (prueba) los tau elegidos en CV; nunca se re-optimizan."""
    exp = ssd_meses(ssd, fecha)
    filas = []
    for _, fila in taus_por_k.iterrows():
        m = metricas_evento(ssd, fecha, dias, score, fila["tau"])
        filas.append({"k": fila["k"], "tau": fila["tau"], **m,
                      "costo": float(costo_por_1000(m["alertas"], m["aciertos"], m["eventos"], exp, fila["k"])),
                      "costo_sin_modelo": 1000.0 * fila["k"] * m["eventos"] / exp})
    return pd.DataFrame(filas)


def k_equilibrio(costos_modelo: pd.DataFrame, costos_baseline: pd.DataFrame) -> float | None:
    """Menor k desde el cual el modelo cuesta menos que el baseline de regla."""
    m = costos_modelo.merge(costos_baseline, on="k", suffixes=("_modelo", "_base"))
    mejor = m.loc[m["costo_modelo"] < m["costo_base"], "k"]
    return float(mejor.min()) if len(mejor) else None


# --------------------------------------------------------------------------------------------
# Evaluación con CV
# --------------------------------------------------------------------------------------------
@dataclass
class ResultadoCV:
    cv_pliegues: list[float]
    train_pliegues: list[float]
    ap_fila_pliegues: list[float]
    segundos_pliegues: list[float]
    oof: pd.DataFrame

    @property
    def resumen(self) -> dict[str, float]:
        cv, tr = np.array(self.cv_pliegues), np.array(self.train_pliegues)
        return {
            "cv_pr_auc_evento_media": float(np.nanmean(cv)),
            "cv_pr_auc_evento_std": float(np.nanstd(cv, ddof=1)) if len(cv) > 1 else 0.0,
            "train_pr_auc_evento": float(np.nanmean(tr)),
            "brecha_train_cv": float(np.nanmean(tr) - np.nanmean(cv)),
            "cv_ap_fila_media": float(np.nanmean(self.ap_fila_pliegues)),
            "segundos_entrenamiento": float(np.sum(self.segundos_pliegues)),
        }


def _score(modelo, X) -> np.ndarray:
    return np.asarray(modelo.predict_proba(X))[:, 1]


def evaluar_cv(familia_o_modelo, df: pd.DataFrame, columnas: list[str],
               splitter: EmbargoTimeSeriesSplit | None = None, params: dict | None = None,
               seed: int = SEMILLA, al_terminar_pliegue: Callable[[int, float], None] | None = None,
               max_filas_metrica_train: int | None = 5_000_000) -> tuple[ResultadoCV, object]:
    """CV propia (no cross_val_score): la métrica por evento necesita SSD_KEY, fecha y DAYS_TO_FAILURE.

    `al_terminar_pliegue(i, valor)` permite a Optuna podar (lanza optuna.TrialPruned).
    La métrica de entrenamiento se calcula sobre los discos de una muestra determinista del
    pliegue de entrenamiento (todos sus eventos incluidos) si supera `max_filas_metrica_train`.
    Devuelve también el modelo del último pliegue (el entrenado con más datos).
    """
    verificar_sin_fuga(columnas)
    splitter = splitter or EmbargoTimeSeriesSplit()
    base = (construir_modelo(familia_o_modelo, params, seed)
            if isinstance(familia_o_modelo, str) else familia_o_modelo)
    X, y = df[columnas], df["TARGET_30D"].to_numpy()
    cv, tr, ap, seg, oofs, modelo = [], [], [], [], [], None
    for i, (itr, iva) in enumerate(splitter.split(df, groups=df["OBSERVATION_DATE"])):
        assert df["OBSERVATION_DATE"].iloc[itr].max() + pd.Timedelta(days=splitter.embargo_dias) \
            < df["OBSERVATION_DATE"].iloc[iva].min(), "Embargo violado"
        modelo = clone(base)
        t0 = time.perf_counter()
        modelo.fit(X.iloc[itr], y[itr])
        seg.append(time.perf_counter() - t0)

        sv = _score(modelo, X.iloc[iva])
        dv = df.iloc[iva]
        cv.append(curva_evento(dv["SSD_KEY"], dv["OBSERVATION_DATE"], dv["DAYS_TO_FAILURE"], sv).pr_auc)
        ap.append(average_precision_score(y[iva], sv) if y[iva].any() else np.nan)

        itr_m = _muestra_discos(df.iloc[itr], max_filas_metrica_train, seed)
        dt = df.iloc[itr].iloc[itr_m] if itr_m is not None else df.iloc[itr]
        st = _score(modelo, dt[columnas])
        tr.append(curva_evento(dt["SSD_KEY"], dt["OBSERVATION_DATE"], dt["DAYS_TO_FAILURE"], st).pr_auc)

        oofs.append(pd.DataFrame({"SSD_KEY": dv["SSD_KEY"].to_numpy(),
                                  "OBSERVATION_DATE": dv["OBSERVATION_DATE"].to_numpy(),
                                  "DAYS_TO_FAILURE": dv["DAYS_TO_FAILURE"].to_numpy(),
                                  "TARGET_30D": y[iva], "score": sv, "pliegue": i}))
        if al_terminar_pliegue is not None:
            al_terminar_pliegue(i, float(np.nanmean(cv)))
    return ResultadoCV(cv, tr, ap, seg, pd.concat(oofs, ignore_index=True)), modelo


def _muestra_discos(d: pd.DataFrame, max_filas: int | None, seed: int):
    """Posiciones de una muestra por disco (todos los que fallan + sanos por hash) o None."""
    if max_filas is None or len(d) <= max_filas:
        return None
    fallan = set(d.loc[d["TARGET_30D"] == 1, "SSD_KEY"])
    h = pd.util.hash_pandas_object(d["SSD_KEY"], index=False, hash_key=f"{seed:016d}").to_numpy()
    frac = max(max_filas - d["SSD_KEY"].isin(fallan).sum(), 0) / len(d)
    keep = d["SSD_KEY"].isin(fallan).to_numpy() | ((h % 10_000) < frac * 10_000)
    return np.flatnonzero(keep)


# --------------------------------------------------------------------------------------------
# MLflow
# --------------------------------------------------------------------------------------------
NOMBRE_EXPERIMENTO = os.getenv("MLFLOW_EXPERIMENTO", "ssd-failure-mc1")
NOMBRE_MODELO_REGISTRO = "ssd-failure-modelo"


def commit_git() -> str:
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        sucio = subprocess.run(["git", "diff", "--quiet", "HEAD"]).returncode != 0
        return sha + ("-dirty" if sucio else "")
    except (OSError, subprocess.CalledProcessError):
        return "desconocido"


def configurar_mlflow(experimento: str = NOMBRE_EXPERIMENTO):
    import mlflow  # noqa: PLC0415

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(experimento)
    return mlflow


def _params_planos(modelo) -> dict[str, str]:
    est = modelo.named_steps["modelo"] if isinstance(modelo, Pipeline) else modelo
    salida = {}
    for k, v in est.get_params(deep=False).items():
        if isinstance(v, BaseEstimator):
            for k2, v2 in v.get_params(deep=False).items():
                salida[f"{k}__{k2}"] = str(v2)[:500]
        else:
            salida[k] = str(v)[:500]
    return salida


def registrar_corrida(*, nombre: str, fase: str, familia: str, modelo, seed: int,
                      metricas: dict[str, float], datos: dict[str, str], X_ejemplo: pd.DataFrame,
                      params_extra: dict | None = None, tags_extra: dict | None = None,
                      nested: bool = False, registrar_modelo: bool = True) -> str:
    """Una corrida con todo lo que pide el PSet #3: params+semilla, datos, métricas, tags, modelo."""
    import mlflow  # noqa: PLC0415
    from mlflow.models import infer_signature  # noqa: PLC0415

    if fase not in {"baseline", "candidato", "tuning", "final"}:
        raise ValueError("fase debe ser baseline, candidato, tuning o final")
    with mlflow.start_run(run_name=nombre, nested=nested) as run:
        mlflow.log_params({"semilla": seed, **_params_planos(modelo), **(params_extra or {}), **datos})
        mlflow.log_metrics({k: v for k, v in metricas.items() if v is not None and np.isfinite(v)})
        mlflow.set_tags({"fase": fase, "familia": familia, "git_commit": commit_git(),
                         **(tags_extra or {})})
        if registrar_modelo:
            ejemplo = X_ejemplo.head(5)
            firma = infer_signature(ejemplo, _score(modelo, ejemplo))
            mlflow.sklearn.log_model(modelo, name="pipeline", signature=firma, input_example=ejemplo,
                                     serialization_format="cloudpickle")
        return run.info.run_id


def exportar_corridas(ruta: str = "../docs/corridas.csv", experimento: str = NOMBRE_EXPERIMENTO) -> pd.DataFrame:
    import mlflow  # noqa: PLC0415

    corridas = mlflow.search_runs(experiment_names=[experimento], max_results=50_000)
    corridas.to_csv(ruta, index=False)
    return corridas
