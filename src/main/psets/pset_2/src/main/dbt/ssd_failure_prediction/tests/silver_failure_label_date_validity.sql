--? TEST SINGULAR (validez): el dataset cubre 2018-2019, asi que una falla fuera de ese rango
--? indica un error de parseo o un dato invalido. Pasa si devuelve 0 filas.
--? ref('modelo') = referencia a otro modelo dbt (crea la dependencia en el DAG).
select silver_record_id
from {{ ref('ssd_failure_labels') }}
where failure_date < '2018-01-01'::date
   or failure_date > '2019-12-31'::date
