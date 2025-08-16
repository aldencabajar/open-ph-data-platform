
select 
{{ dbt_utils.generate_surrogate_key(['psgc', 'name', 'geographic_level']) }} as id,
psgc::string as geo_code,
UPPER(TRIM(REGEXP_REPLACE(name, '\(.*\)', '', 'g'))) as name,
UPPER(TRIM(geographic_level)) as geographic_level,
UPPER(TRIM(urban_rural_class)) as urban_rural_class,
source_uri,
source_timestamp_utc,
NOW() AS updated_date_time_utc

FROM {{ source('raw', 'psa_geographical_codes') }}