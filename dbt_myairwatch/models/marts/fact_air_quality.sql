select
    reading_id,
    source,
    to_hex(md5(to_json_string(struct(
        source_location_id,
        location_name,
        state,
        country,
        latitude,
        longitude
    )))) as location_id,
    pollutant,
    observed_date as date_day,
    reading_value,
    health_risk_category,
    is_high_risk
from {{ ref('int_air_quality_enriched') }}
