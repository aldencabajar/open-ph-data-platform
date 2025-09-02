
SELECT 
    wcm.city_municipality_id as id,
    p.city_municipality_name,
    p.province_id,
    area_in_sqm,
    class
FROM {{ ref('wikipedia_city_municipality__matched') }} wcm
LEFT JOIN (
    SELECT DISTINCT province_id, city_municipality_id, city_municipality_name
    FROM {{ ref("psa_geographical_codes__pivoted") }}
) p USING (city_municipality_id)

