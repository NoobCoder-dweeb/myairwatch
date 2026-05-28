select distinct
    pollutant,
    unit
from {{ ref('int_air_quality_enriched') }}
where pollutant is not null
