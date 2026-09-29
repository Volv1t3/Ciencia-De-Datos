-- File provenance is separated from the trip event because one staged file
-- contributes many facts. The hash is a reproducible file-dimension key.
{{ config(materialized='table') }}

select
    sha2(concat_ws('|', source_month, source_file, source_content_key, source_loaded_at), 256)
        as source_file_key,
    source_month,
    source_file,
    source_content_key,
    source_loaded_at,
    count(*) as curated_trip_count_in_file
from {{ ref('silver_yellow_taxi_trips') }}
group by 1, 2, 3, 4, 5

/*
READ-AFTER-CODE: source-file dimension flow

This model groups Silver facts by their shared ingestion lineage. The hash makes
a compact key from delivery month, staged file, content key, and load time. The
fact retains its source row number, while the repeated file metadata moves here.
*/
