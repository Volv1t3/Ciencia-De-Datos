"""Pruebas locales de contrato en Spark para el generador de la OBT MC1.

Estas pruebas nunca se conectan a Snowflake y no representan una prueba de rendimiento.
Verifican esquemas deterministas, comportamiento de ventanas de calendario, construccion de
etiquetas y compuertas bloqueantes de validacion sobre un fixture sintetico de dos filas.
"""


#? ==========================================================================================
#? TESTS DE CONTRATO SPARK: Validacion unitaria local del generador de OBT MC1 (build_mc1_obt).
#? ------------------------------------------------------------------------------------------
#? Proposito:
#?   Verificar de manera rapida y determinista (sin conexion a Snowflake ni consumo de computo)
#?   que el codigo de transformacion de PySpark cumple con:
#?     1. Contratos de esquema exactos: 847 columnas para OBT_MC1_RN y 429 para OBT_MC1_R / N.
#?     2. Unicidad de columnas y prefijo/sufijo contractual (identidad y etiqueta).
#?     3. Calculo correcto de ventanas moviles basadas en calendario (rangeBetween en dias).
#?     4. Asignacion logica de estados de etiqueta (POSITIVE, NEGATIVE, CENSORED, etc.) y TARGET_30D.
#?     5. Evaluacion de las funciones de validacion de stage (AuditMetrics, conteos no negativos).
#?     6. Paridad exacta de proyecciones entre RN y sus variantes R y N (exceptAll bidireccional).
#?
#? Como se ejecuta:
#?   docker compose --env-file src/res/env/.env run --rm --no-deps \
#?     -v .:/workspace:ro snowpark-connect \
#?     python /workspace/src/test/python/spark/test_build_mc1_obt_contract.py
#? ==========================================================================================
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql import Row, SparkSession


#? Agrega el directorio de jobs al sys.path para poder importar build_mc1_obt sin instalarlo como paquete.
JOBS_DIRECTORY = (
    Path(__file__).resolve().parents[3] / "main" / "python" / "spark" / "jobs"
)
sys.path.insert(0, str(JOBS_DIRECTORY))

import build_mc1_obt as job  # noqa: E402


class Mc1ObtContractTest(unittest.TestCase):
    #? Arranca una sesion Spark local con 1 solo hilo (local[1]) para pruebas ultra-rapidas y deterministas.
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("mc1-obt-contract-test")
            .getOrCreate()
        )

    #? Detiene la sesion Spark al finalizar la suite para liberar recursos de la JVM.
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    #? Construye un fixture sintetico minimo de 2 observaciones (2018-01-01 y 2018-01-02)
    #? para un SSD de prueba ('ssd-1'), con falla programada el 2018-01-20 y visto hasta 2018-02-05.
    #? Esto permite validar ventanas moviles, etiquetado y contratos sin conexion a Snowflake.
    def _build_fixture(self):
        rows = []
        for value, observation_date in enumerate(
            (date(2018, 1, 1), date(2018, 1, 2)), start=1
        ):
            rows.append(
                Row(
                    **{
                        "SMART_DAILY_KEY": f"key-{value}",
                        "SSD_KEY": "ssd-1",
                        "DISK_ID": 1,
                        "MODEL_CODE": "MC1",
                        "OBSERVATION_DATE_KEY": int(
                            observation_date.strftime("%Y%m%d")
                        ),
                        "OBSERVATION_DATE": observation_date,
                        **{
                            column: value
                            for column in job.smart_columns()
                        },
                    }
                )
            )
        observations = self.spark.createDataFrame(rows)
        #? Simula el hecho Gold de fallas: una falla para ssd-1 el dia 20 de enero de 2018.
        failure_summary = self.spark.createDataFrame(
            [
                Row(
                    SSD_KEY="ssd-1",
                    FIRST_FAILURE_DATE=date(2018, 1, 20),
                    FAILURE_EVENT_COUNT=1,
                )
            ]
        )
        #? Simula el ultimo dia visto para el SSD (necesario para evaluar censura a 30 dias).
        last_seen = self.spark.createDataFrame(
            [Row(SSD_KEY="ssd-1", LAST_SEEN_DATE=date(2018, 2, 5))]
        )
        labels = job.build_labels(observations, failure_summary, last_seen)
        arguments = job.JobArguments(
            start_date=None,
            end_date=None,
            ssd_limit=None,
            validate_only=False,
            explain_plan=False,
            keep_staging=False,
        )
        #? Invoca la construccion real del DataFrame de 847 columnas (RN).
        return job.build_rn_obt(observations, labels, arguments)

    #? TEST 1: Valida que los anchos de columnas generados coincidan exactamente con el contrato:
    #?   - OBT_MC1_RN: 847 columnas (6 identidad + 836 features + 5 etiquetas).
    #?   - OBT_MC1_R:  429 columnas (6 identidad + 418 features raw + 5 etiquetas).
    #?   - OBT_MC1_N:  429 columnas (6 identidad + 418 features norm + 5 etiquetas).
    #? Ademas verifica que los nombres de columna no tengan colisiones y comiencen/terminen
    #? con las columnas contractuales fijas.
    def test_deterministic_contract_widths(self) -> None:
        rn_columns = job.rn_final_columns()
        r_columns = job.r_final_columns()
        n_columns = job.n_final_columns()

        self.assertEqual(job.EXPECTED_RN_WIDTH, len(rn_columns))
        self.assertEqual(job.EXPECTED_VARIANT_WIDTH, len(r_columns))
        self.assertEqual(job.EXPECTED_VARIANT_WIDTH, len(n_columns))
        self.assertEqual(len(rn_columns), len(set(rn_columns)))
        self.assertEqual(rn_columns[:6], list(job.COMMON_COLUMNS))
        self.assertEqual(rn_columns[-5:], list(job.TARGET_COLUMNS))

    #? TEST 2: Valida semantica de ventanas moviles, transiciones de etiqueta y validaciones de stage.
    #?   - Ventana 7D: dia 1 tiene conteo 1; dia 2 (consecutivo) tiene conteo 2.
    #?   - Ventana previa 7D: ambos dias tienen conteo 0 (no hay observaciones anteriores).
    #?   - Etiqueta: falla el dia 20 (a 19 y 18 dias de anticipacion) -> POSITIVE con TARGET_30D = 1.
    #?   - validate_rn_stage: asegura que pase las 7 compuertas de auditoria sin errores.
    #?   - validate_variant_parity: valida paridad exacta de proyecciones mediante exceptAll.
    def test_window_labels_and_persisted_validations(self) -> None:
        rn = self._build_fixture()
        result = (
            rn.orderBy("OBSERVATION_DATE")
            .select(
                "R_1_COUNT_7D",
                "R_1_PREV_COUNT_7D",
                "LABEL_STATUS",
                "TARGET_30D",
            )
            .collect()
        )

        #? Comprueba los conteos de filas dentro de la ventana de 7 dias calendario.
        self.assertEqual([1, 2], [row["R_1_COUNT_7D"] for row in result])
        self.assertEqual([0, 0], [row["R_1_PREV_COUNT_7D"] for row in result])
        #? Comprueba que ambas observaciones caen en horizonte de prediccion positivo (1..30 dias).
        self.assertTrue(all(row["LABEL_STATUS"] == "POSITIVE" for row in result))
        self.assertTrue(all(row["TARGET_30D"] == 1 for row in result))

        #? Ejecuta la funcion de validacion de stage de produccion y comprueba metricas de auditoria.
        metrics = job.AuditMetrics()
        job.validate_rn_stage(rn, expected_row_count=2, metrics=metrics)
        self.assertEqual(2, metrics.values["MC1_OBT_ROW_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_ROLLING_COUNT_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_MEAN_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_SHIFT_COUNT"])

        #? Proyecta las variantes R y N y valida su integridad y paridad relacional con RN.
        r_variant = rn.select(*job.r_final_columns())
        n_variant = rn.select(*job.n_final_columns())
        job.validate_variant_stage(r_variant, "R", expected_row_count=2)
        job.validate_variant_stage(n_variant, "N", expected_row_count=2)
        job.validate_variant_parity(rn, r_variant, n_variant)


if __name__ == "__main__":
    unittest.main()
