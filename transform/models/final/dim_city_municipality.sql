WITH psgc_province AS (
    SELECT DISTINCT
        province_id,
        city_municipality_id,
        city_municipality_name
    FROM {{ ref("psa_geographical_codes__pivoted") }}

)

SELECT
    wcm.city_municipality_id AS id,
    p.city_municipality_name,
    p.province_id,
    wcm.area_in_sqm,
    wcm.class
FROM {{ ref('wikipedia_city_municipality__matched') }} AS wcm
LEFT JOIN psgc_province AS p
    ON wcm.city_municipality_id = p.city_municipality_id
