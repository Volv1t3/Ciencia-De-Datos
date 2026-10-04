--? TEST SINGULAR (consistencia): la fecha dentro de la fila (ds) debe estar en 2018 y coincidir
--? con la fecha del nombre del archivo diario (source_date, puesta por la ingesta).
select silver_record_id
from {{ ref('smart_2018') }}
where observation_date < '2018-01-01'::date
   or observation_date > '2018-12-31'::date
   or observation_date <> source_date
