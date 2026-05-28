select
    observed_date,
    state,
    pollutant,
    max(reading_value) as max_reading_value,
    count(*) as high_risk_reading_count
from {{ ref('int_air_quality_enriched') }}
where is_high_risk
group by observed_date, state, pollutant
