WITH prelim_clean as (
    SELECT UPPER(TRIM(REGEXP_REPLACE(province , '\[.*\]', ''))) as province_name,
    UPPER(TRIM(REGEXP_REPLACE(capital, '\[.*\]|[^A-Za-z-\s]', '', 'g'))) as capital,
    CAST(TRIM(REGEXP_REPLACE(area, 'km2\(.*\)|,|\[.*\]', '', 'g')) AS numeric) as area_in_sqm,
    UPPER(TRIM("Island group")) as island_group,
    UPPER(TRIM(REGEXP_REPLACE(region, '\[.*\]', '', 'g'))) as region,
    REGEXP_REPLACE(founded, '\[.*\]', '', 'g') as founded_cleaned,
    load_datetime_utc
    
    from {{ source('raw', 'wikipedia_province_data') }}
    
)

SELECT * exclude(founded_cleaned),
 nullif(regexp_extract(founded_cleaned, '\d{4}'), '') as year_founded,
 strftime(try_strptime(founded_cleaned, '%-d %b %Y'), '%j')::integer as day_of_year_founded
 FROM prelim_clean