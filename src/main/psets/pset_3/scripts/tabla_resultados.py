"""Tabla de resultados para el memo (desde docs/corridas.csv; no entrena ni toca la prueba).

Uso (desde pset_3/):  python scripts/tabla_resultados.py
Escribe docs/tabla_resultados.csv y docs/tabla_resultados.md con, por modelo: fase, familia, PR-AUC por
evento en entrenamiento y en CV (media ± std), brecha train − CV, learning rate y tiempo de
entrenamiento; y aparte las dos corridas finales vigentes con sus métricas de la prueba.
"""
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
c = pd.read_csv(RAIZ / "docs" / "corridas.csv", low_memory=False)
c = c[c.get("tags.estado", pd.Series(index=c.index, dtype=object)) != "reemplazada"]  # solo corridas vigentes
nombre = c["tags.mlflow.runName"].astype(str)


def learning_rate(fila) -> str:
    """Learning rate con que se entrenó; para las familias sin uno, el motivo."""
    familia = fila["tags.familia"]
    for col in ("params.learning_rate", "params.lr"):
        v = fila.get(col)
        if pd.notna(v) and str(v) not in ("None", "nan"):
            v = float(v)
            if familia == "bosque":
                return f"{v:g} (bagging: sin boosting)"
            return f"{v:g}"
    return {"xgboost": "0.3 (por defecto de XGBoost)",
            "logistica": "no aplica (quasi-Newton; regularización C)",
            "sin_temporales": "no aplica (quasi-Newton; regularización C)",
            "tabpfn": "no aplica (aprende en contexto, sin entrenar)",
            "regla": "no aplica (umbral sobre R_198)",
            "dummy": "no aplica"}.get(familia, "no aplica")


principales = c[c["tags.fase"].isin(["baseline", "candidato"]) | nombre.str.endswith("_optimizado")]
principales = principales[principales["tags.familia"] != "llm"]
tabla = pd.DataFrame({
    "modelo": nombre[principales.index],
    "fase": principales["tags.fase"],
    "familia": principales["tags.familia"],
    "train_pr_auc_evento": principales["metrics.train_pr_auc_evento"],
    "cv_media": principales["metrics.cv_pr_auc_evento_media"],
    "cv_std": principales["metrics.cv_pr_auc_evento_std"],
    "brecha_train_cv": principales["metrics.brecha_train_cv"],
    "learning_rate": principales.apply(learning_rate, axis=1),
    "minutos_entrenamiento": principales["metrics.segundos_entrenamiento"] / 60,
    "run_id": principales["run_id"],
}).sort_values("cv_media", ascending=False)

vigentes = c[(c["tags.fase"] == "final") & (c.get("tags.estado") == "vigente")]
finales = pd.DataFrame({
    "rol": vigentes["tags.rol"], "familia": vigentes["tags.familia"],
    "cv_media": vigentes["metrics.cv_pr_auc_evento_media"],
    "test_pr_auc_evento": vigentes["metrics.test_pr_auc_evento"],
    "test_precision_evento": vigentes["metrics.test_precision_evento"],
    "test_recall_evento": vigentes["metrics.test_recall_evento"],
    "test_f05_evento": vigentes["metrics.test_f05_evento"],
    "learning_rate": vigentes.apply(learning_rate, axis=1),
    "minutos_entrenamiento": vigentes["metrics.segundos_entrenamiento"] / 60,
    "inferencia_s_por_millon": vigentes["metrics.inferencia_s_por_millon"],
    "run_id": vigentes["run_id"],
})

tabla.to_csv(RAIZ / "docs" / "tabla_resultados.csv", index=False)
fmt = {"train_pr_auc_evento": "{:.4f}", "cv_media": "{:.4f}", "cv_std": "{:.4f}", "brecha_train_cv": "{:+.4f}",
       "minutos_entrenamiento": "{:.1f}", "test_pr_auc_evento": "{:.4f}", "test_precision_evento": "{:.3f}",
       "test_recall_evento": "{:.3f}", "test_f05_evento": "{:.3f}", "inferencia_s_por_millon": "{:.1f}"}


def a_md(df):
    d = df.drop(columns=["run_id"]).copy()
    for col, f in fmt.items():
        if col in d:
            d[col] = d[col].map(lambda v: f.format(v) if pd.notna(v) else "—")
    filas = ["| " + " | ".join(d.columns) + " |", "|" + "---|" * len(d.columns)]
    filas += ["| " + " | ".join(str(v) for v in r) + " |" for r in d.itertuples(index=False)]
    return "\n".join(filas)


(RAIZ / "docs" / "tabla_resultados.md").write_text(
    "## Resultados en la CV temporal (PR-AUC por evento)\n\n" + a_md(tabla)
    + "\n\n## Corridas finales vigentes (prueba única, noviembre de 2019)\n\n" + a_md(finales) + "\n",
    encoding="utf-8")
print(a_md(tabla))
print()
print(a_md(finales))
