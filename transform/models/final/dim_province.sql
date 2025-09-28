SELECT
    psgc.id,
    psgc.name AS province_name,
    psgc.geo_code,
    wiki.capital,
    wiki.area_in_sqm,
    wiki.island_group,
    wiki.region,
    wiki.year_founded,
    wiki.day_of_year_founded
FROM {{ ref('psa_geographical_codes') }} AS psgc
LEFT JOIN {{ ref('wikipedia_province_data') }} AS wiki
    ON psgc.name = wiki.province_name
WHERE psgc.geographic_level = 'PROV'
