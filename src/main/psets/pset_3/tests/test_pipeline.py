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


def test_episodios_se_repiten_tras_30_dias():
    # Alerta a 45 días de la falla (falsa alarma). A 10 días ya pasó el episodio de 30 días:
    # se abre otro y acierta. Con la lectura literal (un episodio por disco) no hay segundo.
    fecha = pd.date_range("2019-01-01", periods=36)
    dias = np.arange(45, 9, -1)
    score = np.where((dias == 45) | (dias == 10), 0.9, 0.0)
    m = pl.metricas_evento(["A"] * 36, fecha, dias, score, tau=0.5)
    assert m["alertas"] == 2 and m["aciertos"] == 1 and m["recall_evento"] == 1.0
    literal = pl.metricas_evento(["A"] * 36, fecha, dias, score, tau=0.5, episodio_dias=None)
    assert literal["alertas"] == 1 and literal["aciertos"] == 0


def test_dentro_del_episodio_no_se_vuelve_a_alertar():
    # Score alto todos los días: un episodio cada 30 días, no una alerta diaria.
    fecha = pd.date_range("2019-01-01", periods=90)
    m = pl.metricas_evento(["A"] * 90, fecha, [np.nan] * 90, np.ones(90), tau=0.5)
    assert m["alertas"] == 3 and m["aciertos"] == 0 and m["eventos"] == 0


def _fuerza_bruta(d, tau, episodio_dias):
    alertas = aciertos = 0
    for _, g in d.sort_values("OBSERVATION_DATE").groupby("SSD_KEY"):
        libre = pd.Timestamp.min
        for f, x, s in zip(g["OBSERVATION_DATE"], g["DAYS_TO_FAILURE"], g["score"]):
            if s >= tau and f >= libre:
                alertas += 1
                aciertos += int(7 <= x <= 30)
                libre = pd.Timestamp.max if episodio_dias is None else f + pd.Timedelta(days=episodio_dias)
    return alertas, aciertos


@pytest.mark.parametrize("episodio_dias", [30, None])
def test_curva_evento_coincide_con_fuerza_bruta(df, episodio_dias):
    rng = np.random.default_rng(1)
    d = df[df["SSD_KEY"].isin([f"d{i}" for i in range(120)])]
    d = d.assign(score=np.round(rng.random(len(d)) * 0.3 + d["R_5"] / 50, 2))
    curva = pl.curva_evento(d["SSD_KEY"], d["OBSERVATION_DATE"], d["DAYS_TO_FAILURE"], d["score"],
                            episodio_dias=episodio_dias)
    for i in rng.choice(len(curva.umbrales), 12, replace=False):
        alertas, aciertos = _fuerza_bruta(d, curva.umbrales[i], episodio_dias)
        assert curva.alertas[i] == alertas and curva.aciertos[i] == aciertos
    assert 0 <= curva.pr_auc <= 1


def test_fechas_como_dias_enteros_equivalen_a_datetime(df):
    s = df["R_5"].to_numpy()
    dias_int = df["OBSERVATION_DATE"].to_numpy().astype("datetime64[D]").astype(np.int32)
    a = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s)
    b = pl.curva_evento(df["SSD_KEY"], dias_int, df["DAYS_TO_FAILURE"], s)
    assert np.array_equal(a.alertas, b.alertas) and a.pr_auc == pytest.approx(b.pr_auc)
    # y con episodios repetibles hay más alertas que con la lectura literal
    lit = pl.curva_evento(df["SSD_KEY"], dias_int, df["DAYS_TO_FAILURE"], s, episodio_dias=None)
    assert b.alertas[-1] > lit.alertas[-1]


def test_muestra_por_disco_conserva_fallas_y_discos_completos(df):
    pos = pl.muestra_por_disco(df, frac_sanos=0.2, seed=7)
    m = df.iloc[pos]
    fallan = set(df.loc[df["TARGET_30D"] == 1, "SSD_KEY"])
    assert fallan <= set(m["SSD_KEY"])                          # todas las fallas
    por_disco = df.groupby("SSD_KEY").size()
    vistos = m.groupby("SSD_KEY").size()
    assert (vistos.to_numpy() == por_disco[vistos.index].to_numpy()).all()  # discos completos
    sanos = set(df["SSD_KEY"]) - fallan
    frac = len(set(m["SSD_KEY"]) - fallan) / len(sanos)
    assert 0.1 < frac < 0.3
    # determinista y estable si SSD_KEY es categórica
    cat = df.assign(SSD_KEY=df["SSD_KEY"].astype("category"))
    assert np.array_equal(pos, pl.muestra_por_disco(cat, 0.2, seed=7))


def test_derivadas_numpy_igual_a_pandas(df):
    cols = pl.columnas_modelo(df, "R")
    a = pl.FeaturesDerivadas().fit(df[cols]).transform(df[cols])
    b = pl.FeaturesDerivadas(salida="numpy").fit(df[cols]).transform(df[cols])
    assert b.dtype == np.float32 and np.allclose(a.to_numpy(np.float32), b, equal_nan=True)


def test_cv_con_submuestra_de_sanos_valida_con_todo(df):
    cols = pl.columnas_modelo(df, "R")
    sp = pl.EmbargoTimeSeriesSplit(n_splits=2, meses_validacion=2)
    completo, _ = pl.evaluar_cv("logistica", df, cols, sp)
    sub, _ = pl.evaluar_cv("logistica", df, cols, sp, frac_sanos_train=0.3)
    assert len(sub.oof) == len(completo.oof)  # validación intacta
    assert sub.resumen["cv_pr_auc_evento_media"] > 0


def test_cache_coincide_con_cargar_dataset(tmp_path):
    pytest.importorskip("polars")
    snap = tmp_path / "snap"
    filas = []
    for mes, dias in (("2019-08", ["2019-08-30", "2019-08-31"]), ("2019-09", ["2019-09-01", "2019-09-30"])):
        (snap / f"mes={mes}").mkdir(parents=True)
        d = pd.DataFrame({"SSD_KEY": ["b", "a"] * len(dias), "OBSERVATION_DATE": pd.to_datetime(dias * 2).date,
                          "DAYS_TO_FAILURE": [np.nan, 10.0] * len(dias), "TARGET_30D": [0.0, 1.0] * len(dias),
                          "R_5": np.arange(2 * len(dias), dtype=float), "N_5": 1.0,
                          "DAYS_SINCE_FIRST_SEEN": 3.0})
        d.to_parquet(snap / f"mes={mes}" / "d.parquet")
        filas.append(d)
    datos.escribir_manifiesto(snap, tabla="T", sql="s", extraido_en="hoy")
    datos.construir_cache(snap, tmp_path / "cache", "R")
    c = datos.cargar_cache(tmp_path / "cache", hasta=datos.FIN_ENTRENAMIENTO)
    e = datos.cargar_dataset(snap, ["R_5", "DAYS_SINCE_FIRST_SEEN"])
    assert list(c.columns) == ["SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D",
                               "R_5", "DAYS_SINCE_FIRST_SEEN"]  # sin columnas N
    assert (c["SSD_KEY"].astype(str).to_numpy() == e["SSD_KEY"].astype(str).to_numpy()).all()
    assert np.array_equal(c["R_5"].to_numpy(), e["R_5"].to_numpy())
    assert (c["OBSERVATION_DATE"].to_numpy() == e["OBSERVATION_DATE"].to_numpy()).all()
    solo_sep = datos.cargar_cache(tmp_path / "cache", desde=pd.Timestamp("2019-09-01").date())
    assert len(solo_sep) == 4 and solo_sep["OBSERVATION_DATE"].min() == pd.Timestamp("2019-09-01")


@pytest.mark.parametrize("familia", ["xgboost", "bosque"])
def test_xgb_balanceado_pickle_y_clone(df, familia):
    pytest.importorskip("xgboost")
    import pickle
    from sklearn.base import clone
    cols = pl.columnas_modelo(df, "R")
    m = pl.construir_modelo(familia, {"device": "cpu", "n_estimators": 5})
    m.fit(df[cols], df["TARGET_30D"])
    est = m.named_steps["modelo"]
    assert est.scale_pos_weight == "balanced"            # el parámetro pedido no se pierde
    assert type(est).__module__ == "src.pipeline"
    m2 = pickle.loads(pickle.dumps(m))                    # pickle encuentra la clase por nombre
    assert np.allclose(m2.predict_proba(df[cols])[:, 1], m.predict_proba(df[cols])[:, 1])
    assert clone(m).named_steps["modelo"].get_params()["scale_pos_weight"] == "balanced"


def test_mlp_entrena_con_etiqueta_entera_y_hace_pickle(df):
    pytest.importorskip("torch")
    import pickle
    cols = pl.columnas_modelo(df, "R")
    m = pl.construir_modelo("mlp", {"max_epochs": 1, "batch_size": 1024})
    m.fit(df[cols], df["TARGET_30D"].astype(np.int8))           # como llega desde la caché
    p = m.predict_proba(df[cols])[:, 1]
    assert p.shape == (len(df),) and np.isfinite(p).all()
    m2 = pickle.loads(pickle.dumps(m))
    assert np.allclose(m2.predict_proba(df[cols])[:, 1], p, atol=1e-6)


def test_tabpfn_no_supera_500_features(df):
    cols = pl.columnas_modelo(df, "R")
    salida = pl.construir_preprocesador("tabpfn").fit_transform(df[cols])
    assert salida.shape[1] <= 500


def test_llm_no_ve_etiquetas_y_usa_cache(df, tmp_path):
    pytest.importorskip("google.genai")
    import json as _json
    from src import llm
    caso = llm.resumen_caso(df.iloc[0])
    texto = _json.dumps(caso)
    for prohibida in pl.COLUMNAS_PROHIBIDAS:
        assert prohibida not in texto                      # sin etiquetas, fechas ni SSD_KEY
    X = df.iloc[:3]
    lote = [{"id": i, **llm.resumen_caso(f)} for i, (_, f) in enumerate(X.iterrows())]
    respuesta = _json.dumps([{"id": i, "probabilidad": p} for i, p in enumerate([0.1, 0.9, 0.5])])
    (tmp_path / f"{llm._clave(llm.MODELO_LLM, lote)}.json").write_text(
        _json.dumps({"respuesta": respuesta, "segundos": 1.0, "tokens": [10, 5, 0]}))
    r = llm.predecir(X, tmp_path, api_key="no-se-usa")      # todo sale de la caché: no llama a la API
    assert r.llamadas_api == 0 and r.respuestas_cache == 1
    assert np.allclose(r.probabilidades, [0.1, 0.9, 0.5])


def test_experimentos_registra_en_mlflow_y_reutiliza(df, tmp_path):
    mlflow = pytest.importorskip("mlflow")
    pytest.importorskip("sqlalchemy")  # backend sqlite de MLflow (está en el contenedor)
    from src import experimentos as exp
    mlflow.set_tracking_uri(f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    mlflow.set_experiment(mlflow.create_experiment("prueba", artifact_location=(tmp_path / "art").as_uri()))
    ctx = exp.Contexto(train=df, columnas=pl.columnas_modelo(df, "R"),
                       splitter=pl.EmbargoTimeSeriesSplit(n_splits=2, meses_validacion=2),
                       oof_dir=tmp_path / "oof", meta_datos={"datos_sha256": "abc"}, variante="R")
    ctx.oof_dir.mkdir()
    r = exp.correr(ctx, "dummy", "baseline", "dummy")
    assert (ctx.oof_dir / "dummy.parquet").exists() and len(r["cv_pliegues"]) == 2
    assert exp.correr(ctx, "dummy", "baseline", "dummy")["run_id"] == r["run_id"]   # no re-entrena
    run = mlflow.get_run(r["run_id"])
    assert run.data.tags["fase"] == "baseline" and run.data.tags["familia"] == "dummy"
    assert run.data.params["semilla"] == str(pl.SEMILLA) and run.data.params["datos_sha256"] == "abc"
    assert "cv_pr_auc_evento_media" in run.data.metrics and "cv_pr_auc_evento_std" in run.data.metrics
    assert set(exp.resultados(ctx.oof_dir)) == {"dummy"}


def test_peso_de_disco_equivale_a_duplicarlo(df):
    rng = np.random.default_rng(5)
    d = df.assign(score=rng.random(len(df)))
    sanos = set(d["SSD_KEY"]) - set(d.loc[d["TARGET_30D"] == 1, "SSD_KEY"])
    copia = d[d["SSD_KEY"].isin(sanos)].assign(SSD_KEY=lambda x: x["SSD_KEY"] + "_copia")
    duplicado = pd.concat([d, copia])
    a = pl.curva_evento(duplicado["SSD_KEY"], duplicado["OBSERVATION_DATE"], duplicado["DAYS_TO_FAILURE"],
                        duplicado["score"])
    peso = np.where(d["SSD_KEY"].isin(sanos), 2.0, 1.0)
    prep = pl.preparar_eventos(d["SSD_KEY"], d["OBSERVATION_DATE"], d["DAYS_TO_FAILURE"], peso)
    b = pl.curva_evento(None, None, None, d["score"], prep=prep)
    umbral = a.umbrales[len(a.umbrales) // 2]   # comparar en un umbral común
    ma = pl.metricas_evento(duplicado["SSD_KEY"], duplicado["OBSERVATION_DATE"], duplicado["DAYS_TO_FAILURE"],
                            duplicado["score"], umbral)
    mb = pl.metricas_evento(None, None, None, d["score"], umbral, prep=prep)
    assert ma["alertas"] == mb["alertas"] and ma["aciertos"] == mb["aciertos"]
    assert b.pr_auc == pytest.approx(a.pr_auc, rel=0.05)


def test_metrica_train_no_se_infla_con_submuestra_de_sanos(df):
    cols = pl.columnas_modelo(df, "R")
    sp = pl.EmbargoTimeSeriesSplit(n_splits=2, meses_validacion=2)
    completo, _ = pl.evaluar_cv("dummy", df, cols, sp)
    sub, _ = pl.evaluar_cv("dummy", df, cols, sp, frac_sanos_train=0.25)
    # el dummy no aprende: su "train" con pesos debe parecerse al de la población completa
    assert sub.resumen["train_pr_auc_evento"] == pytest.approx(completo.resumen["train_pr_auc_evento"], rel=0.5)


def test_tuning_corridas_anidadas_y_retoma(df, tmp_path):
    optuna = pytest.importorskip("optuna")
    mlflow = pytest.importorskip("mlflow")
    pytest.importorskip("sqlalchemy")
    from src import experimentos as exp, tuning
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    mlflow.set_tracking_uri(f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    mlflow.set_experiment(mlflow.create_experiment("t", artifact_location=(tmp_path / "art").as_uri()))
    ctx = exp.Contexto(train=df, columnas=pl.columnas_modelo(df, "R"),
                       splitter=pl.EmbargoTimeSeriesSplit(n_splits=2, meses_validacion=2),
                       oof_dir=tmp_path / "oof", meta_datos={"datos_sha256": "abc"}, variante="R",
                       frac_sanos=0.5)
    storage = f"sqlite:///{(tmp_path / 'optuna.db').as_posix()}"
    st = tuning.tunear(ctx, "logistica", storage, completas=3)
    assert len(st.trials) == 3 and np.isfinite(st.best_value)
    padre = st.user_attrs["mlflow_run_padre"]
    hijas = mlflow.search_runs(filter_string=f"tags.mlflow.parentRunId = '{padre}'")
    assert len(hijas) == 3 and set(hijas["tags.fase"]) == {"tuning"}
    st2 = tuning.tunear(ctx, "logistica", storage, completas=3)   # retoma: no agrega configuraciones
    assert len(st2.trials) == 3 and st2.user_attrs["mlflow_run_padre"] == padre


@pytest.mark.parametrize("indicador", [True, False])
def test_preprocesador_numerico_igual_a_sklearn(indicador):
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer, StandardScaler
    rng = np.random.default_rng(0)
    X = rng.lognormal(0, 3, size=(5000, 8)) * rng.choice([-1, 1], size=(5000, 8))
    X[rng.random(X.shape) < 0.2] = np.nan
    X[:, 3] = np.nan                       # columna sin datos: se descarta
    X[:, 5] = np.where(np.isnan(X[:, 5]), 7.0, X[:, 5])  # sin faltantes: no lleva indicador
    X[:, 6] = 2.5                          # constante: escala 1
    X = pd.DataFrame(X.astype(np.float32), columns=[f"c{j}" for j in range(8)])
    ref = Pipeline([("log1p", FunctionTransformer(pl._log1p_con_signo, feature_names_out="one-to-one")),
                    ("imputar", SimpleImputer(strategy="median", add_indicator=indicador, keep_empty_features=False)),
                    ("escalar", StandardScaler())]).fit(X)
    nuevo = pl.PreprocesadorNumerico(add_indicator=indicador).fit(X)
    a, b = ref.transform(X), nuevo.transform(X)
    assert a.shape == b.shape
    assert np.allclose(a, b, atol=1e-4)
    Xn = X.sample(frac=1.0, random_state=1)  # datos nuevos (orden distinto): mismas estadísticas
    assert np.allclose(ref.transform(Xn), nuevo.transform(Xn), atol=1e-4)


def test_cache_de_preprocesamiento_da_el_mismo_resultado(df):
    cols = pl.columnas_modelo(df, "R")
    sp = pl.EmbargoTimeSeriesSplit(n_splits=2, meses_validacion=2)
    sin, _ = pl.evaluar_cv("logistica", df, cols, sp, frac_sanos_train=0.5)
    cache = {}
    con, modelo = pl.evaluar_cv("logistica", df, cols, sp, frac_sanos_train=0.5, cache_prep=cache)
    assert len(cache) == 2
    assert np.allclose(con.cv_pliegues, sin.cv_pliegues) and np.allclose(con.train_pliegues, sin.train_pliegues)
    otra, _ = pl.evaluar_cv("logistica", df, cols, sp, {"C": 0.1}, frac_sanos_train=0.5, cache_prep=cache)
    assert len(cache) == 2                                  # reutiliza: no agrega entradas
    assert modelo.predict_proba(df[cols]).shape == (len(df), 2)   # el Pipeline devuelto es completo


def test_commit_git_ignora_salidas_de_notebooks_y_docs(tmp_path, monkeypatch):
    import shutil
    import subprocess
    if shutil.which("git") is None:
        pytest.skip("sin git")
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "docs").mkdir()
    (repo / "notebooks").mkdir()
    (repo / "src" / "a.py").write_text("x = 1\n")
    (repo / "docs" / "r.json").write_text("{}")
    (repo / "notebooks" / "n.ipynb").write_text("{}")
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(git + ["-C", str(repo), "add", "."], check=True)
    subprocess.run(git + ["-C", str(repo), "commit", "-qm", "c"], check=True)
    monkeypatch.chdir(repo / "notebooks")
    assert not pl.commit_git().endswith("-dirty")
    (repo / "docs" / "r.json").write_text('{"x": 1}')            # salida regenerada
    (repo / "notebooks" / "n.ipynb").write_text('{"celdas": 1}')  # notebook ejecutado
    assert not pl.commit_git().endswith("-dirty")
    (repo / "src" / "a.py").write_text("x = 2\n")                # cambio de código
    assert pl.commit_git().endswith("-dirty")


def test_umbral_por_presupuesto_no_usa_etiquetas_ni_escala(df):
    s = df["R_5"].to_numpy() + np.random.default_rng(0).random(len(df)) * 1e-3
    exp = pl.ssd_meses(df["SSD_KEY"], df["OBSERVATION_DATE"])
    curva = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s)
    objetivo = float(pl.presupuesto(curva.alertas[len(curva.alertas) // 3], exp))
    tau = pl.umbral_para_presupuesto(df["SSD_KEY"], df["OBSERVATION_DATE"], s, objetivo, exp)
    m = pl.metricas_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s, tau)
    assert pl.presupuesto(m["alertas"], exp) <= objetivo + 1e-9
    assert pl.presupuesto(m["alertas"], exp) >= 0.8 * objetivo        # cerca del presupuesto
    # Una transformación monótona del score (otra escala) da exactamente las mismas alertas
    s2 = np.log1p(s) * 7 + 3
    tau2 = pl.umbral_para_presupuesto(df["SSD_KEY"], df["OBSERVATION_DATE"], s2, objetivo, exp)
    m2 = pl.metricas_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s2, tau2)
    assert m2["alertas"] == m["alertas"] and m2["aciertos"] == m["aciertos"]
    # Sin presupuesto no se alerta
    assert pl.umbral_para_presupuesto(df["SSD_KEY"], df["OBSERVATION_DATE"], s, 0.0, exp) == np.inf


def test_costo_con_presupuesto(df):
    s = df["R_5"].to_numpy()
    exp = pl.ssd_meses(df["SSD_KEY"], df["OBSERVATION_DATE"])
    curva = pl.curva_evento(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s)
    p = pl.presupuestos_por_k(curva, exp, ks=[1, 10, 50])
    c = pl.costo_con_presupuesto(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"], s * 100, p)
    assert (c["alertas_por_1000"] <= c["alertas_por_1000_cv"] + 1e-9).all()
    assert c["alertas"].is_monotonic_increasing          # fallas más caras -> se alerta más


def test_prep_reutilizable_y_orden_de_filas_irrelevante(df):
    s = df["R_5"].to_numpy()
    prep = pl.preparar_eventos(df["SSD_KEY"], df["OBSERVATION_DATE"], df["DAYS_TO_FAILURE"])
    a = pl.curva_evento(None, None, None, s, prep=prep).pr_auc
    p = np.random.default_rng(3).permutation(len(df))
    d2 = df.iloc[p]
    b = pl.curva_evento(d2["SSD_KEY"], d2["OBSERVATION_DATE"], d2["DAYS_TO_FAILURE"], s[p]).pr_auc
    assert a == pytest.approx(b)


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
