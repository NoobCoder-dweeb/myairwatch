with readings as (
    select * from {{ ref('stg_opendosm') }}
    union all
    select * from {{ ref('stg_openaq') }}
)

select
    to_hex(md5(to_json_string(struct(
        source,
        source_location_id,
        location_name,
        pollutant,
        observed_at
    )))) as reading_id,
    source,
    source_location_id,
    location_name,
    coalesce(state, 'Unknown') as state,
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
    health_risk_category,
    case
        when health_risk_category in ('unhealthy', 'very_unhealthy', 'hazardous')
            then true
        else false
    end as is_high_risk
from readings
