select distinct
    to_hex(md5(to_json_string(struct(
        source_location_id,
        location_name,
        state,
        country,
        latitude,
        longitude
    )))) as location_id,
    source_location_id,
    location_name,
    state,
    country,
    latitude,
    longitude
from {{ ref('int_air_quality_enriched') }}
