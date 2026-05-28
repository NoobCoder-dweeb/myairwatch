select
    source,
    source_location_id,
    location_name,
    state,
    country,
    latitude,
    longitude,
    pollutant,
    unit,
    reading_value,
    observed_at,
    observed_date,
    year,
    month,
    day,
    health_risk_category
from {{ source('myairwatch_staging', 'air_quality_readings') }}
where source = 'openaq'
