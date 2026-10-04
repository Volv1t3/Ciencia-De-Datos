--? TABLA DE HECHOS SMART DIARIO (solo MC1).
--? GRAIN: una fila = un SSD fisico (disk_id + model_code) en un dia de observacion.
--? Medidas: las 44 columnas SMART n_X/r_X (sin transformar). Claves foraneas: ssd_key -> dim_ssd,
--? observation_date_key -> dim_date. Lo comprueban los tests de models/gold/schema.yml:
--?   unique (ssd_key, observation_date), relationships hacia ambas dimensiones y
--?   gold_row_count_matches_source (mismo numero de filas que smart_mc1_all_years).
{{
    config(
        alias='FCT_SMART_DAILY_MC1',
        tags=['gold', 'fact', 'smart', 'mc1']
    )
}}

-- Column-lineage contract:
-- * All 44 retained MC1 SMART R_x/N_x measures pass through unchanged.
-- * DISK_ID + MODEL_CODE resolve to DIM_SSD.SSD_KEY and are not duplicated here.
-- * OBSERVATION_DATE remains visible and also resolves to DIM_DATE through
--   OBSERVATION_DATE_KEY.
-- * SMART_DAILY_KEY replaces the Silver row identifier at the Gold business
--   grain of one physical SSD on one observation date.
-- * File/archive/hash/load metadata remains in Silver; it is operational
--   lineage, not dimensional analytical context.
select
    --? Clave del hecho: hash(ssd_key | fecha). Determinista: la misma fila siempre recibe la misma clave.
    sha2(
        concat_ws('|', ssd.ssd_key, to_char(smart.observation_date, 'YYYY-MM-DD')),
        256
    ) as smart_daily_key,
    ssd.ssd_key,
    date_dimension.date_key as observation_date_key,
    smart.observation_date,
    {{ mc1_smart_feature_columns('smart') }}
--? INNER JOIN es seguro: dim_ssd y dim_date se construyen a partir de estas mismas tablas, asi que
--? toda fila encuentra su dimension. Si alguna se perdiera, el test de conteo de filas fallaria.
from {{ ref('smart_mc1_all_years') }} as smart
inner join {{ ref('dim_ssd') }} as ssd
    on smart.disk_id = ssd.disk_id
   and smart.model_code = ssd.model_code
inner join {{ ref('dim_date') }} as date_dimension
    on smart.observation_date = date_dimension.full_date
