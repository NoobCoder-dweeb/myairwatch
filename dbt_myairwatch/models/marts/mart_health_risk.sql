select
    observed_date,
    pollutant,
    health_risk_category,
    count(*) as reading_count,
    avg(reading_value) as avg_reading_value
from {{ ref('int_air_quality_enriched') }}
group by observed_date, pollutant, health_risk_category
