{{
    config(
        alias='FCT_SMART_DAILY_ALL',
        tags=['gold', 'fact', 'smart', 'all_models']
    )
}}

-- Column-lineage contract:
-- * All 68 retained SMART R_x/N_x measures pass through unchanged.
-- * DISK_ID + MODEL_CODE resolve to DIM_SSD.SSD_KEY and are not duplicated here.
-- * OBSERVATION_DATE remains visible and also resolves to DIM_DATE through
--   OBSERVATION_DATE_KEY.
-- * SMART_DAILY_KEY replaces the Silver row identifier at the Gold business
--   grain of one physical SSD on one observation date.
-- * File/archive/hash/load metadata remains in Silver; it is operational
--   lineage, not dimensional analytical context.
select
    sha2(
        concat_ws('|', ssd.ssd_key, to_char(smart.observation_date, 'YYYY-MM-DD')),
        256
    ) as smart_daily_key,
    ssd.ssd_key,
    date_dimension.date_key as observation_date_key,
    smart.observation_date,
    {{ retained_smart_feature_columns('smart') }}
from {{ ref('smart_null_processed_all_years') }} as smart
inner join {{ ref('dim_ssd') }} as ssd
    on smart.disk_id = ssd.disk_id
   and smart.model_code = ssd.model_code
inner join {{ ref('dim_date') }} as date_dimension
    on smart.observation_date = date_dimension.full_date
