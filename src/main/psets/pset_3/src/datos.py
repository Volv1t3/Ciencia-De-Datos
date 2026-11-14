"""Extracción de la OBT MC1 desde Snowflake, snapshot versionado y partición temporal.

Flujo:
    extraer_obt()      Snowflake --COPY INTO--> stage de usuario (Parquet por mes) --GET por mes--> disco local
    escribir_manifiesto()  SHA-256 de cada archivo + hash global del snapshot + conteos
    cargar_dataset()   lee el snapshot, verifica el hash y devuelve un DataFrame float32
    split_holdout()    entrenamiento/CV | embargo | prueba (particiones acordadas por el equipo)

Las columnas de etiqueta (DAYS_TO_FAILURE, TARGET_30D) se descargan porque las métricas por
evento las necesitan, pero NUNCA son features: ver `COLUMNAS_PROHIBIDAS` en pipeline.py.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------------
# Contrato de la OBT (mismo que pset_2/src/main/python/spark/jobs/build_mc1_obt.py)
# --------------------------------------------------------------------------------------------
SMART_IDS: tuple[int, ...] = (
    1, 5, 9, 12, 170, 171, 172, 173, 174, 180, 183,
    184, 187, 188, 194, 195, 196, 197, 198, 199, 206, 211,
)
VENTANAS: tuple[int, ...] = (7, 14, 30)

COLUMNAS_IDENTIDAD: tuple[str, ...] = (
    "SMART_DAILY_KEY", "SSD_KEY", "DISK_ID", "MODEL_CODE", "OBSERVATION_DATE_KEY", "OBSERVATION_DATE",
)
COLUMNAS_ETIQUETA: tuple[str, ...] = (
    "FIRST_FAILURE_DATE", "LAST_SEEN_DATE", "DAYS_TO_FAILURE", "LABEL_STATUS", "TARGET_30D",
)
# Lo que se descarga además de las features: clave del disco, fecha y lo necesario para evaluar.
COLUMNAS_META: tuple[str, ...] = ("SSD_KEY", "OBSERVATION_DATE", "DAYS_TO_FAILURE", "TARGET_30D")

# Feature derivada que se calcula en SQL porque necesita el historial completo del disco.
FEATURE_EDAD = "DAYS_SINCE_FIRST_SEEN"

# --------------------------------------------------------------------------------------------
# Particiones acordadas por el equipo (mc1_manifiesto_experimento.json, prototipo en pset_2).
# En el PSet #3 la validación es por CV dentro de ENTRENAMIENTO; la prueba se usa una sola vez.
# --------------------------------------------------------------------------------------------
INICIO_DATOS = date(2018, 1, 1)
FIN_ENTRENAMIENTO = date(2019, 9, 30)
EMBARGO = (date(2019, 10, 1), date(2019, 10, 31))
INICIO_PRUEBA = date(2019, 11, 1)
FIN_PRUEBA = date(2019, 11, 30)

_IDENTIFICADOR = re.compile(r'^("?[A-Za-z_][A-Za-z0-9_$]*"?)(\."?[A-Za-z_][A-Za-z0-9_$]*"?){0,2}$')


@dataclass(frozen=True)
class ConfigSnowflake:
    account: str
    user: str
    password: str
    role: str | None
    warehouse: str
    tabla_obt: str  # nombre completo, p. ej. S_CDATOS_PSET2.S_CDATOS_PSET2_OBT.OBT_MC1_R

    @staticmethod
    def desde_entorno() -> "ConfigSnowflake":
        faltan = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                              "SNOWFLAKE_WAREHOUSE", "OBT_TABLA") if not os.getenv(v)]
        if faltan:
            raise RuntimeError(f"Faltan variables de entorno: {', '.join(faltan)} (ver .env.example)")
        tabla = os.environ["OBT_TABLA"]
        if not _IDENTIFICADOR.match(tabla):
            raise ValueError(f"OBT_TABLA no es un identificador válido: {tabla!r}")
        return ConfigSnowflake(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.getenv("SNOWFLAKE_ROLE") or None,
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            tabla_obt=tabla,
        )


def conectar(cfg: ConfigSnowflake | None = None):
    """Abre una conexión con snowflake-connector-python (import perezoso)."""
    import snowflake.connector  # noqa: PLC0415

    cfg = cfg or ConfigSnowflake.desde_entorno()
    return snowflake.connector.connect(
        account=cfg.account, user=cfg.user, password=cfg.password,
        role=cfg.role, warehouse=cfg.warehouse,
        session_parameters={"QUERY_TAG": "pset3-extraccion"},
    )


def columnas_features_obt(conn, tabla: str) -> list[str]:
    """Columnas SMART de la OBT (todo lo que no es identidad ni etiqueta), en orden físico."""
    partes = [p.strip('"') for p in tabla.split(".")]
    if len(partes) != 3:
        raise ValueError("OBT_TABLA debe ser BASE.ESQUEMA.TABLA")
    base, esquema, nombre = partes
    cur = conn.cursor()
    cur.execute(
        f'SELECT COLUMN_NAME FROM "{base}".INFORMATION_SCHEMA.COLUMNS '
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (esquema, nombre),
    )
    todas = [r[0] for r in cur.fetchall()]
    if not todas:
        raise RuntimeError(f"No se encontró la tabla {tabla} o no hay permisos de lectura")
    excluir = set(COLUMNAS_IDENTIDAD) | set(COLUMNAS_ETIQUETA)
    return [c for c in todas if c not in excluir]


def sql_extraccion(tabla: str, columnas_features: list[str]) -> str:
    """SELECT que se descarga.

    - `DAYS_SINCE_FIRST_SEEN` se calcula ANTES de filtrar por etiqueta, sobre todas las
      observaciones del disco, con una ventana que solo mira al pasado (filas anteriores o
      iguales a la fecha): existe en el momento de la inferencia. Es edad *observada*: el
      dataset empieza el 2018-01-03, así que los discos más viejos están censurados por la
      izquierda.
    - Solo se descargan filas con TARGET_30D conocido (POSITIVE / NEGATIVE).
    - Las features se convierten a FLOAT para que el Parquet tenga dobles y no DECIMAL.
    """
    feats = ",\n        ".join(f'"{c}"::FLOAT AS "{c}"' for c in columnas_features)
    return f"""
SELECT
        "SSD_KEY",
        "OBSERVATION_DATE",
        "DAYS_TO_FAILURE"::FLOAT AS "DAYS_TO_FAILURE",
        "TARGET_30D"::FLOAT AS "TARGET_30D",
        "{FEATURE_EDAD}"::FLOAT AS "{FEATURE_EDAD}",
        {feats}
FROM (
    SELECT o.*,
           DATEDIFF(
               'day',
               MIN("OBSERVATION_DATE") OVER (
                   PARTITION BY "SSD_KEY" ORDER BY "OBSERVATION_DATE"
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ),
               "OBSERVATION_DATE"
           ) AS "{FEATURE_EDAD}"
    FROM {tabla} o
)
WHERE "TARGET_30D" IS NOT NULL
""".strip()


def _filas_parquet(directorio: Path) -> int:
    import pyarrow.parquet as pq
    return sum(pq.ParquetFile(p).metadata.num_rows for p in directorio.rglob("*.parquet"))


def extraer_obt(directorio: str | Path, cfg: ConfigSnowflake | None = None,
                prefijo_stage: str = "pset3_obt", copiar: bool = True) -> Path:
    """Descarga la OBT a `directorio` como Parquet particionado por mes y escribe el manifiesto.

    Usa COPY INTO al stage de usuario (@~) y GET: es mucho más rápido y barato que traer
    ~100M filas por cursor. GET aplana las carpetas del stage y los archivos de meses distintos
    pueden llamarse igual, así que se baja cada partición `mes=AAAA-MM/` a su propia carpeta.
    El stage solo se borra cuando las filas descargadas coinciden con las que reportó COPY INTO.
    `copiar=False` reutiliza lo que ya está en el stage (reintento de una descarga fallida).
    """
    cfg = cfg or ConfigSnowflake.desde_entorno()
    destino = Path(directorio).resolve()
    destino.mkdir(parents=True, exist_ok=True)
    if any(destino.rglob("*.parquet")):
        raise FileExistsError(f"{destino} ya tiene Parquet; usa un directorio vacío por snapshot")

    conn = conectar(cfg)
    try:
        cur = conn.cursor()
        columnas = columnas_features_obt(conn, cfg.tabla_obt)
        sql = sql_extraccion(cfg.tabla_obt, columnas)
        stage = f"@~/{prefijo_stage}/"
        if copiar:
            cur.execute(f"REMOVE {stage}")
            cur.execute(
                f"COPY INTO {stage} FROM ({sql}) "
                "PARTITION BY ('mes=' || TO_VARCHAR(\"OBSERVATION_DATE\", 'YYYY-MM')) "
                "FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY) "
                "HEADER = TRUE MAX_FILE_SIZE = 268435456"
            )
            print("COPY INTO:", cur.fetchall(), flush=True)

        # LIST devuelve nombres como 'pset3_obt/mes=2018-01/data_....snappy.parquet'
        archivos = [r[0] for r in cur.execute(f"LIST {stage}").fetchall()]
        if not archivos:
            raise RuntimeError(f"El stage {stage} está vacío")
        particiones = sorted({a.split("/")[1] for a in archivos if a.count("/") >= 2})
        print(f"{len(archivos)} archivos en {len(particiones)} particiones", flush=True)
        for part in particiones:
            sub = destino / part
            sub.mkdir(exist_ok=True)
            cur.execute(f"GET '{stage}{part}/' 'file://{sub.as_posix()}/' PARALLEL = 16")
            print("GET", part, flush=True)

        # Verificación: lo que se bajó == lo que se exportó (filas del SELECT de extracción).
        esperadas = cur.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        bajadas = _filas_parquet(destino)
        if bajadas != esperadas:
            raise RuntimeError(f"Descarga incompleta: {bajadas} filas locales vs {esperadas} en Snowflake; "
                               f"el stage se conserva para reintentar con copiar=False")
        cur.execute(f"REMOVE {stage}")
        cur.execute("SELECT CURRENT_TIMESTAMP()::STRING")
        extraido_en = cur.fetchone()[0]
    finally:
        conn.close()

    escribir_manifiesto(destino, tabla=cfg.tabla_obt, sql=sql, extraido_en=extraido_en)
    return destino


# --------------------------------------------------------------------------------------------
# Snapshot: manifiesto y hash
# --------------------------------------------------------------------------------------------
def _sha256_archivo(ruta: Path, bloque: int = 1 << 24) -> str:
    h = hashlib.sha256()
    with ruta.open("rb") as f:
        while chunk := f.read(bloque):
            h.update(chunk)
    return h.hexdigest()


def hash_snapshot(directorio: str | Path) -> tuple[str, dict[str, str]]:
    """Hash global = SHA-256 de las líneas 'ruta_relativa sha256' ordenadas por ruta."""
    base = Path(directorio)
    archivos = sorted(base.rglob("*.parquet"))
    if not archivos:
        raise FileNotFoundError(f"No hay Parquet en {base}")
    por_archivo = {p.relative_to(base).as_posix(): _sha256_archivo(p) for p in archivos}
    lineas = "\n".join(f"{k} {v}" for k, v in sorted(por_archivo.items()))
    return hashlib.sha256(lineas.encode()).hexdigest(), por_archivo


def escribir_manifiesto(directorio: Path, *, tabla: str, sql: str, extraido_en: str) -> dict:
    hash_global, por_archivo = hash_snapshot(directorio)
    import pyarrow.dataset as ds
    meta = ds.dataset([str(p) for p in archivos_snapshot(directorio)], format="parquet"
                      ).to_table(columns=list(COLUMNAS_META)).to_pandas()
    eventos = meta.loc[meta["TARGET_30D"] == 1, "SSD_KEY"].nunique()
    manifiesto = {
        "tabla_origen": tabla,
        "extraido_en": extraido_en,
        "fecha_corte": str(pd.to_datetime(meta["OBSERVATION_DATE"]).max().date()),
        "fecha_inicio": str(pd.to_datetime(meta["OBSERVATION_DATE"]).min().date()),
        "filas": int(len(meta)),
        "filas_positivas": int((meta["TARGET_30D"] == 1).sum()),
        "ssd_distintos": int(meta["SSD_KEY"].nunique()),
        "eventos_falla": int(eventos),
        "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
        "dataset_sha256": hash_global,
        "archivos": por_archivo,
    }
    (directorio / "manifiesto.json").write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False))
    (directorio / "extraccion.sql").write_text(sql)
    return manifiesto


def leer_manifiesto(directorio: str | Path) -> dict:
    return json.loads((Path(directorio) / "manifiesto.json").read_text())


def metadatos_dataset(directorio: str | Path) -> dict[str, str]:
    """Lo que cada corrida de MLflow registra como 'datos' (tabla, fecha de corte, hash)."""
    m = leer_manifiesto(directorio)
    return {
        "datos_tabla_origen": m["tabla_origen"],
        "datos_fecha_corte": m["fecha_corte"],
        "datos_sha256": m["dataset_sha256"],
        "datos_filas": str(m["filas"]),
    }


def archivos_snapshot(directorio: str | Path) -> list[Path]:
    """Los Parquet del snapshot (la carpeta también tiene manifiesto.json y extraccion.sql)."""
    return sorted(Path(directorio).rglob("*.parquet"))


def cargar_dataset(directorio: str | Path, columnas: list[str] | None = None,
                   verificar_hash: bool = True, desde: date | None = None,
                   hasta: date | None = None) -> pd.DataFrame:
    """Lee el snapshot. Features en float32 (la mitad de memoria); verifica el hash por defecto.

    `desde`/`hasta` (inclusive) filtran por OBSERVATION_DATE al leer, sin cargar el resto.
    """
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    base = Path(directorio)
    if verificar_hash:
        esperado = leer_manifiesto(base)["dataset_sha256"]
        obtenido, _ = hash_snapshot(base)
        if obtenido != esperado:
            raise ValueError(f"El snapshot cambió: hash {obtenido} != manifiesto {esperado}")
    if columnas is not None:
        columnas = list(dict.fromkeys(list(COLUMNAS_META) + list(columnas)))
    dataset = ds.dataset([str(p) for p in archivos_snapshot(base)], format="parquet")
    filtro = None
    for limite, op in ((desde, pc.greater_equal), (hasta, pc.less_equal)):
        if limite is not None:
            f = op(ds.field("OBSERVATION_DATE"), pa_fecha(limite))
            filtro = f if filtro is None else filtro & f
    df = dataset.to_table(columns=columnas, filter=filtro).to_pandas()
    df["OBSERVATION_DATE"] = pd.to_datetime(df["OBSERVATION_DATE"])
    feats = [c for c in df.columns if c not in COLUMNAS_META and c != "SSD_KEY"]
    df[feats] = df[feats].astype(np.float32)
    df["TARGET_30D"] = df["TARGET_30D"].astype(np.int8)
    return df.sort_values(["OBSERVATION_DATE", "SSD_KEY"], kind="stable").reset_index(drop=True)


def pa_fecha(d: date):
    import pyarrow as pa
    return pa.scalar(d, type=pa.date32())


# --------------------------------------------------------------------------------------------
# Partición única
# --------------------------------------------------------------------------------------------
def split_holdout(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entrenamiento (2018-01 → 2019-09) y prueba (2019-11). Octubre de 2019 es embargo.

    El embargo de 30 días existe porque la etiqueta de un día t mira hasta t+30: sin él, las
    últimas filas de entrenamiento compartirían la falla que se intenta predecir en prueba.
    """
    fechas = df["OBSERVATION_DATE"].dt.date
    train = df[(fechas >= INICIO_DATOS) & (fechas <= FIN_ENTRENAMIENTO)]
    prueba = df[(fechas >= INICIO_PRUEBA) & (fechas <= FIN_PRUEBA)]
    hueco = (pd.Timestamp(INICIO_PRUEBA) - pd.Timestamp(FIN_ENTRENAMIENTO)).days
    assert hueco > 30, "El embargo entre entrenamiento y prueba debe superar 30 días"
    return train.reset_index(drop=True), prueba.reset_index(drop=True)
