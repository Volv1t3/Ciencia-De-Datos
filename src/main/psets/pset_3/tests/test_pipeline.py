"""Pruebas de la lógica que no depende de Snowflake ni de GPU.

Ejecutar desde pset_3/:  python -m pytest -q tests
"""

import numpy as np
import pandas as pd
import pytest

from src import datos, pipeline as pl


# ------------------------------------------------------------------ datos sintéticos
def _sintetico(n_discos=300, dias=500, seed=0, prob_falla=0.15):
    """Discos diarios; los que fallan degradan R_5 en sus últimos 40 días."""
    rng = np.random.default_rng(seed)
    filas = []
    inicio = pd.Timestamp("2018-01-01")
    for d in range(n_discos):
        falla = rng.random() < prob_falla
        dia_falla = int(rng.integers(120, dias + 40)) if falla else None
        for t in range(dias):
            if dia_falla is not None and t >= dia_falla:
                break
            dtf = (dia_falla - t) if dia_falla is not None else np.nan
            if dia_falla is None and t > dias - 31:
                continue  # censurado: sin futuro suficiente
            if dia_falla is not None and dia_falla > dias - 1 and t > dias - 31:
                continue
            degr = max(0, 40 - dtf) if dia_falla is not None and dtf <= 40 else 0
            r5 = rng.poisson(0.2) + degr * rng.uniform(0.5, 1.5)
            filas.append({
                "SSD_KEY": f"d{d}", "OBSERVATION_DATE": inicio + pd.Timedelta(days=t),
                "DAYS_TO_FAILURE": dtf if dia_falla is not None else np.nan,
                "TARGET_30D": int(dia_falla is not None and 1 <= dtf <= 30),
                "R_5": r5, "R_5_MEAN_SHIFT_30D": degr / 10 + rng.normal(0, 0.1),
                "R_9": t + rng.normal(0, 1), "R_9_MEAN_30D": t + rng.normal(0, 1),
                "R_187_MEAN_SHIFT_30D": rng.normal(0, 1),
                "DAYS_SINCE_FIRST_SEEN": float(t),
            })
    return pd.DataFrame(filas)


@pytest.fixture(scope="module")
def df():
    return _sintetico()


# ------------------------------------------------------------------ métricas por evento
def test_alerta_con_anticipacion_7_a_30_es_acierto():
    fechas = pd.date_range("2019-01-01", periods=4)
    # disco A falla el día 13 (dtf 12..9): alerta el día 1 -> 12 días -> acierto
    # disco B falla el día 6 (dtf 5..2): alerta -> tardía, no acierto
    # disco C no falla: alerta -> falso positivo
    ssd = ["A"] * 4 + ["B"] * 4 + ["C"] * 4
    fecha = list(fechas) * 3
    dias = [12, 11, 10, 9, 5, 4, 3, 2] + [np.nan] * 4
    score = [0.9, 0.1, 0.1, 0.1, 0.9, 0.9, 0.9, 0.9, 0.95, 0.1, 0.1, 0.1]
    m = pl.metricas_evento(ssd, fecha, dias, score, tau=0.5)
    assert m["alertas"] == 3 and m["aciertos"] == 1 and m["alertas_tardias"] == 1
    assert m["eventos"] == 1  # B nunca estuvo a 7+ días de su falla: no es anticipable
    assert m["precision_evento"] == pytest.approx(1 / 3)
    assert m["recall_evento"] == 1.0


def test_solo_cuenta_la_primera_alerta():
    # El disco alerta a 45 días de la falla (no acierto); la segunda alerta a 10 días no cuenta.
    fecha = pd.date_range("2019-01-01", periods=36)
    dias = np.arange(45, 9, -1)
    score = np.where((dias == 45) | (dias == 10), 0.9, 0.0)
    m = pl.metricas_evento(["A"] * 36, fecha, dias, score, tau=0.5)
    assert m["alertas"] == 1 and m["aciertos"] == 0


def _curva_fuerza_bruta(d, tau):
    alerta = d[d["score"] >= tau].sort_values("OBSERVATION_DATE").groupby("SSD_KEY").head(1)
    return len(alerta), int(alerta["DAYS_TO_FAILURE"].between(7, 30).sum())


def test_curva_evento_coincide_con_definicion_literal(df):
    rng = np.random.default_rng(1)
    d = df.assign(score=np.round(rng.random(len(df)) * 0.3 + df["R_5"] / 50, 2))
    curva = pl.curva_evento(d["SSD_KEY"], d["OBSERVATION_DATE"], d["DAYS_TO_FAILURE"], d["score"])
    for i in rng.choice(len(curva.umbrales), 25, replace=False):
        alertas, aciertos = _curva_fuerza_bruta(d, curva.umbrales[i])
        assert curva.alertas[i] == alertas and curva.aciertos[i] == aciertos
    assert 0 <= curva.pr_auc <= 1


def test_pr_auc_score_perfecto_y_aleatorio(df):
    perfecto = df["DAYS_TO_FAILURE"].between(7, 30).astype(float)  # solo alerta en la ventana
    c = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], perfecto)
    assert c.pr_auc == pytest.approx(1.0)
    azar = np.random.default_rng(2).random(len(df))
    c2 = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], azar)
    assert c2.pr_auc < 0.5


# ------------------------------------------------------------------ negocio
def test_barrer_k_y_costo(df):
    s = df["R_5"].to_numpy()
    c = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s)
    exp = pl.ssd_meses(df["SSD_KEY"], df["OBSERVATION_DATE"])
    t = pl.barrer_k(c, exp, ks=[1, 10, 50])
    assert (t["costo"] <= t["costo_sin_modelo"] + 1e-9).all()
    assert t["alertas"].is_monotonic_increasing  # fallas más caras -> se alerta más
    en_otro = pl.costo_con_tau(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s, t)
    assert np.allclose(en_otro["costo"], t["costo"])  # mismo conjunto -> mismo costo


# ------------------------------------------------------------------ validación y fuga
def test_splitter_respeta_embargo_y_es_expansivo(df):
    sp = pl.EmbargoTimeSeriesSplit(n_splits=3, meses_validacion=2, embargo_dias=30)
    previo = 0
    for itr, iva in sp.split(df, groups=df["OBSERVATION_DATE"]):
        f = df["OBSERVATION_DATE"]
        assert f.iloc[itr].max() + pd.Timedelta(days=30) < f.iloc[iva].min()
        assert len(itr) > previo
        previo = len(itr)


def test_columnas_prohibidas_no_entran():
    with pytest.raises(AssertionError):
        pl.verificar_sin_fuga(["R_5", "DAYS_TO_FAILURE"])
    cols = pl.columnas_modelo(pd.DataFrame(columns=["SSD_KEY", "TARGET_30D", "R_5", "N_5",
                                                    "DAYS_SINCE_FIRST_SEEN", "DAYS_TO_FAILURE"]), "R")
    assert cols == ["R_5", "DAYS_SINCE_FIRST_SEEN"]


def test_feature_derivada_solo_usa_el_dia(df):
    """Truncar el futuro no cambia el valor de la feature en t (sin fuga)."""
    cols = pl.columnas_modelo(df, "R")
    corte = df["OBSERVATION_DATE"].quantile(0.5)
    t = pl.FeaturesDerivadas().fit(df[cols])
    completo = t.transform(df[cols])[pl.FEATURE_DEGRADACION]
    truncado = df[df["OBSERVATION_DATE"] <= corte]
    parcial = t.transform(truncado[cols])[pl.FEATURE_DEGRADACION]
    assert np.array_equal(completo.loc[truncado.index].to_numpy(), parcial.to_numpy())


def test_sql_edad_mira_solo_al_pasado_y_filtra_despues():
    sql = datos.sql_extraccion("DB.SCH.OBT_MC1_R", ["R_5", "R_5_MEAN_7D"])
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql
    assert sql.index("OVER (") < sql.index('WHERE "TARGET_30D" IS NOT NULL')


def test_embargo_entre_entrenamiento_y_prueba():
    f = pd.date_range("2019-08-01", "2019-12-31")
    d = pd.DataFrame({"OBSERVATION_DATE": f})
    tr, te = datos.split_holdout(d)
    assert (te["OBSERVATION_DATE"].min() - tr["OBSERVATION_DATE"].max()).days > 30


def test_hash_snapshot_estable(tmp_path):
    pytest.importorskip("pyarrow")
    pd.DataFrame({"a": [1, 2]}).to_parquet(tmp_path / "x.parquet")
    h1, _ = datos.hash_snapshot(tmp_path)
    h2, _ = datos.hash_snapshot(tmp_path)
    assert h1 == h2
    pd.DataFrame({"a": [1, 3]}).to_parquet(tmp_path / "x.parquet")
    assert datos.hash_snapshot(tmp_path)[0] != h1


def test_cargar_dataset_ignora_no_parquet_y_filtra_fechas(tmp_path):
    pytest.importorskip("pyarrow")
    for mes, fechas in (("2019-09", ["2019-09-29", "2019-09-30"]), ("2019-11", ["2019-11-02"])):
        (tmp_path / f"mes={mes}").mkdir()
        pd.DataFrame({"SSD_KEY": ["a"] * len(fechas), "OBSERVATION_DATE": pd.to_datetime(fechas).date,
                      "DAYS_TO_FAILURE": np.nan, "TARGET_30D": 0.0, "R_5": 1.0}
                     ).to_parquet(tmp_path / f"mes={mes}" / "d.parquet")
    datos.escribir_manifiesto(tmp_path, tabla="T", sql="select 1", extraido_en="hoy")
    todo = datos.cargar_dataset(tmp_path)
    assert len(todo) == 3 and todo["R_5"].dtype == np.float32
    train = datos.cargar_dataset(tmp_path, ["R_5"], hasta=datos.FIN_ENTRENAMIENTO)
    assert train["OBSERVATION_DATE"].max() == pd.Timestamp("2019-09-30") and len(train) == 2


# ------------------------------------------------------------------ CV de punta a punta
@pytest.mark.parametrize("familia", ["dummy", "regla", "sin_temporales", "logistica"])
def test_evaluar_cv_familias_cpu(df, familia):
    cols = pl.columnas_modelo(df, "R")
    params = {"atributo": "R_5"} if familia == "regla" else None
    sp = pl.EmbargoTimeSeriesSplit(n_splits=3, meses_validacion=2)
    res, modelo = pl.evaluar_cv(familia, df, cols, sp, params=params)
    r = res.resumen
    assert len(res.cv_pliegues) == 3 and r["segundos_entrenamiento"] >= 0
    assert set(res.oof["pliegue"]) == {0, 1, 2}
    if familia in {"regla", "logistica"}:
        assert r["cv_pr_auc_evento_media"] > 0.3, r  # la señal sintética está en R_5


def test_modelo_supera_dummy(df):
    cols = pl.columnas_modelo(df, "R")
    sp = pl.EmbargoTimeSeriesSplit(n_splits=3, meses_validacion=2)
    dummy = pl.evaluar_cv("dummy", df, cols, sp)[0].resumen["cv_pr_auc_evento_media"]
    logi = pl.evaluar_cv("logistica", df, cols, sp)[0].resumen["cv_pr_auc_evento_media"]
    assert logi > dummy
