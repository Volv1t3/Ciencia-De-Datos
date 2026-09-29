# Infrastructure architecture

```mermaid
flowchart LR
    TLC[NYC TLC Parquet files] --> Raw[src/res/data/raw]
    Raw --> Kestra[Kestra]
    Kestra --> Bronze[Snowflake BRONZE]
    Bronze --> DBT[dbt Core through dbt-ui]
    DBT --> Silver[Snowflake S_CDATOS_SILVER]
    Silver --> Gold[Snowflake GOLD]
    PG[(Local PostgreSQL)] --> Kestra
```

The arrows from Kestra to Snowflake and through dbt show the intended pipeline.
The ingestion flow and dbt models are later work; this scaffold starts the local
services and prepares their code and configuration locations.
