
select psgc::string as geo_code,
UPPER(TRIM(REGEXP_REPLACE(name, '\(.*\)', '', 'g'))) as name,
UPPER(TRIM(geographic_level)) as geographic_level,
source_uri,
source_timestamp_utc,
NOW() AS updated_date_time_utc

FROM {{ source('raw', 'psa_geographical_codes') }}