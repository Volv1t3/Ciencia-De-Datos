"""Small local-Spark contract tests for the MC1 OBT generator.

These tests never connect to Snowflake and do not represent a performance test.
They verify deterministic schemas, calendar-window behavior, label construction,
and the blocking validation expressions on a two-row fixture.
"""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql import Row, SparkSession


JOBS_DIRECTORY = (
    Path(__file__).resolve().parents[3] / "main" / "python" / "spark" / "jobs"
)
sys.path.insert(0, str(JOBS_DIRECTORY))

import build_mc1_obt as job  # noqa: E402


class Mc1ObtContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("mc1-obt-contract-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

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
        failure_summary = self.spark.createDataFrame(
            [
                Row(
                    SSD_KEY="ssd-1",
                    FIRST_FAILURE_DATE=date(2018, 1, 20),
                    FAILURE_EVENT_COUNT=1,
                )
            ]
        )
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
        return job.build_rn_obt(observations, labels, arguments)

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

        self.assertEqual([1, 2], [row["R_1_COUNT_7D"] for row in result])
        self.assertEqual([0, 0], [row["R_1_PREV_COUNT_7D"] for row in result])
        self.assertTrue(all(row["LABEL_STATUS"] == "POSITIVE" for row in result))
        self.assertTrue(all(row["TARGET_30D"] == 1 for row in result))

        metrics = job.AuditMetrics()
        job.validate_rn_stage(rn, expected_row_count=2, metrics=metrics)
        self.assertEqual(2, metrics.values["MC1_OBT_ROW_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_ROLLING_COUNT_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_MEAN_COUNT"])
        self.assertEqual(0, metrics.values["INVALID_SHIFT_COUNT"])

        r_variant = rn.select(*job.r_final_columns())
        n_variant = rn.select(*job.n_final_columns())
        job.validate_variant_stage(r_variant, "R", expected_row_count=2)
        job.validate_variant_stage(n_variant, "N", expected_row_count=2)
        job.validate_variant_parity(rn, r_variant, n_variant)


if __name__ == "__main__":
    unittest.main()
