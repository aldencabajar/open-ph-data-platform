
WITH province_city_municipality_geo AS (
    SELECT DISTINCT
        province_id,
        province_name,
        province_geo_code,
        city_municipality_id,
        city_municipality_name,
        city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes__pivoted') }}
)


/*
SELECT  
    wcm.* EXCLUDE (province_name, city_municipality_name),
    wcm.city_municipality_name,
    wcm.province_name,
    COALESCE(pcmg.city_municipality_id, pcmg2.city_municipality_id) AS city_municipality_id,
    COALESCE(pcmg.province_id, pcmg2.province_id) AS province_id
FROM {{ ref('wikipedia_city_municipality') }} wcm
LEFT JOIN province_city_municipality_geo pcmg
ON wcm.city_municipality_name = pcmg.city_municipality_name
AND wcm.province_name = pcmg.province_name

LEFT JOIN province_city_municipality_geo pcmg2
ON {{ normalize_city_name('pcmg2.city_municipality_name') }} LIKE CONCAT('%', {{ normalize_city_name('wcm.city_municipality_name') }}, '%')
AND wcm.province_name = pcmg2.province_name
*/

SELECT  
    wcm.* EXCLUDE (province_name, city_municipality_name),
    wcm.city_municipality_name,
    wcm.province_name,
    pcmg.city_municipality_id AS city_municipality_id,
    pcmg.province_id AS province_id
FROM {{ ref('wikipedia_city_municipality') }} wcm
LEFT JOIN province_city_municipality_geo pcmg
ON {{ normalize_city_name('wcm.city_municipality_name') }} = {{ normalize_city_name('pcmg.city_municipality_name') }}
AND IFNULL(wcm.province_name, 'DEFAULT') = IFNULL(pcmg.province_name, 'DEFAULT')