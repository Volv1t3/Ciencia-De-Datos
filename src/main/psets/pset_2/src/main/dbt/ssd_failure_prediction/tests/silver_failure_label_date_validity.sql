select silver_record_id
from {{ ref('ssd_failure_labels') }}
where failure_date < '2018-01-01'::date
   or failure_date > '2019-12-31'::date
