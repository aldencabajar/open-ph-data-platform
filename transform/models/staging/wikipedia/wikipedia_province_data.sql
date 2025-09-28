WITH prelim_clean AS (
    SELECT
        load_datetime_utc,
        UPPER(TRIM(REGEXP_REPLACE(province, '\[.*\]', ''))) AS province_name,
        UPPER(TRIM(REGEXP_REPLACE(capital, '\[.*\]|[^A-Za-z-\s]', '', 'g')))
            AS capital,
        CAST(
            TRIM(REGEXP_REPLACE(area, 'km2\(.*\)|,|\[.*\]', '', 'g')) AS numeric
        ) AS area_in_sqm,
        UPPER(TRIM("Island group")) AS island_group,
        UPPER(TRIM(REGEXP_REPLACE(region, '\[.*\]', '', 'g'))) AS region,
        REGEXP_REPLACE(founded, '\[.*\]', '', 'g') AS founded_cleaned

    FROM {{ source('raw', 'wikipedia_province_data') }}

)

SELECT
    * EXCLUDE (founded_cleaned),
    NULLIF(REGEXP_EXTRACT(founded_cleaned, '\d{4}'), '') AS year_founded,
    CAST(STRFTIME(
        TRY_STRPTIME(founded_cleaned, '%-d %b %Y'), '%j'
    ) AS integer) AS day_of_year_founded
FROM prelim_clean
