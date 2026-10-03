# SSD Failure Prediction

This context describes the storage-device identities and observations used by
the SMART telemetry and failure-prediction dataset.

## Language

**Physical SSD**:
A storage device represented in the dataset by the combination of its disk serial and censored model code.
_Avoid_: SSD model, disk model

**Disk ID**:
The dataset-provided disk serial. It is unique within a model population but may be reused by a different model code.
_Avoid_: Global SSD identifier

**Model Code**:
A censored SSD product-model identifier shared by multiple physical SSDs.
_Avoid_: Disk ID, SSD serial

**SSD Business Key**:
The pair `(DISK_ID, MODEL_CODE)`, which uniquely identifies a physical SSD within this dataset.
_Avoid_: `DISK_ID` alone

**SSD Key**:
A surrogate identifier for one SSD business key, used to relate an SSD consistently across analytical observations and events.
_Avoid_: Disk ID, model code

**SMART Observation**:
The SMART telemetry recorded for one SSD business key on one observation date.
_Avoid_: SSD, failure event

**Failure Event**:
A confirmed SSD failure recorded independently from SMART observations, including when no telemetry exists on the failure date.
_Avoid_: Failure target, SMART observation

**MC1 OBT Observation**:
One MC1 SMART observation enriched with calendar-window features and one
30-day failure-label state. It retains the same SSD-day grain as its Gold SMART
observation.
_Avoid_: SSD lifetime, failure event, trained example

**MC1 OBT Build Run**:
One attempted construction of all three MC1 predictor representations (`R`,
`N`, and `RN`) from the same observation and label populations.
_Avoid_: One run per output table, model-training run
