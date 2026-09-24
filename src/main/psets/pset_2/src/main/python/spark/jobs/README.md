# Spark jobs

`build_obt.py` is intentionally a connectivity skeleton. The future contract is
Snowflake GOLD → Spark DataFrames → joins and OBT validation → Snowflake OBT.
It currently starts local Spark, reports runtime versions, and checks that the
Snowflake connector class is on the classpath without reading or writing any
Snowflake table.
