select
    observed_date,
    state,
    pollutant,
    count(*) as reading_count,
    avg(reading_value) as avg_reading_value,
    countif(is_high_risk) as high_risk_reading_count
from {{ ref('int_air_quality_enriched') }}
group by observed_date, state, pollutant
