
WITH province_city_municipality_geo AS (
    SELECT DISTINCT
        province_name,
        city_municipality_id,
        city_municipality_name
    FROM {{ ref('psa_geographical_codes__pivoted') }}
)


SELECT  
    wcm.* EXCLUDE (province_name, city_municipality_name),
    wcm.city_municipality_name,
    wcm.province_name,
    pcmg.city_municipality_id AS city_municipality_id
FROM {{ ref('wikipedia_city_municipality') }} wcm
LEFT JOIN province_city_municipality_geo pcmg
ON {{ normalize_city_name('wcm.city_municipality_name') }} = {{ normalize_city_name('pcmg.city_municipality_name') }}
AND IFNULL(wcm.province_name, 'DEFAULT') = IFNULL(pcmg.province_name, 'DEFAULT')