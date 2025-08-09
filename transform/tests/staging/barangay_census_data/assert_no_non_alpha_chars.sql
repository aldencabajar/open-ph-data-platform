SELECT *
FROM (
    SELECT 
        SUM(REGEXP_MATCHES(city_municipality, '[^a-zA-Z-\s]')) as non_alpha_count_city_municipality,
        SUM(REGEXP_MATCHES(province, '[^a-zA-Z-\s]')) as non_alpha_count_province,
        SUM(REGEXP_MATCHES(region, '[^a-zA-Z-\s]')) as non_alpha_count_region
    FROM {{ ref('barangay_census_data') }}
)
WHERE non_alpha_count_city_municipality > 0
OR non_alpha_count_province > 0
OR non_alpha_count_region > 0