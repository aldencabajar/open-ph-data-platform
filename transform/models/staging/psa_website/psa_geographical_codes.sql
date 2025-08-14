
{{
  config(
    materialized = 'incremental',
    )
}}

select psgc::string as geo_code,
UPPER(TRIM(REGEXP_REPLACE(name, '\(.*\)', '', 'g'))) as name,
UPPER(TRIM(geographic_level)) as geographic_level,
source_uri,
source_timestamp_utc,
NOW() AS updated_date_time_utc

FROM {{ source('bronze', 'psa_geographical_codes') }}

{% if is_incremental() %}
  WHERE load_datetime_utc >= coalesce((select max(updated_date_time_utc) from {{ this }}), '1900-01-01')
{% endif %}