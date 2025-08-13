select psgc::string as psgc,
UPPER(TRIM(REGEXP_REPLACE(name, '\(.*\)', '', 'g'))) as name,
UPPER(TRIM(geographic_level)) as geographic_level,
source_uri,
source_timestamp_utc
FROM {{ source('bronze', 'psa_geographical_codes') }}