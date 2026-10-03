select silver_record_id
from {{ ref('smart_2018') }}
where observation_date < '2018-01-01'::date
   or observation_date > '2018-12-31'::date
   or observation_date <> source_date
