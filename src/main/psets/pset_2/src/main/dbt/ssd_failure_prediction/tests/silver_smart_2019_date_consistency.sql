--? TEST SINGULAR (consistencia): ds en 2019 e igual a la fecha del archivo diario.
select silver_record_id
from {{ ref('smart_2019') }}
where observation_date < '2019-01-01'::date
   or observation_date > '2019-12-31'::date
   or observation_date <> source_date
