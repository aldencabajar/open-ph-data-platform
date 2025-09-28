WITH preproc AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['psgc', 'name', 'geographic_level']) }} AS id, --noqa: LT05
        psgc::string AS geo_code,
        UPPER(TRIM(REGEXP_REPLACE(name, '\(.*\)', '', 'g'))) AS name,
        UPPER(TRIM(geographic_level)) AS geographic_level,
        UPPER(TRIM(urban_rural_class)) AS urban_rural_class,
        source_uri,
        source_timestamp_utc

    FROM {{ source('raw', 'psa_geographical_codes') }}
)

SELECT
    id,
    geo_code,
    name,
    geographic_level,
    source_uri,
    source_timestamp_utc,
    CASE WHEN urban_rural_class = '-' THEN 'UNK' ELSE urban_rural_class END
        AS urban_rural_class
FROM preproc
