select distinct
    observed_date as date_day,
    year,
    month,
    day
from {{ ref('int_air_quality_enriched') }}
where observed_date is not null
