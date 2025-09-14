
WITH province_city_municipality_geo AS (
    SELECT DISTINCT
        COALESCE(map.wikipedia_province_name, psgc.province_name) AS province_name,
        city_municipality_id,
        COALESCE(map.wikipedia_city_mun_name, psgc.city_municipality_name) AS city_municipality_name
    FROM {{ ref('psa_geographical_codes__pivoted') }} psgc
    LEFT JOIN {{ ref('psgc_wikipedia_city_mun_mapping') }} map
    ON psgc.city_municipality_name = map.psgc_city_mun_name
    AND IFNULL(psgc.province_name, 'DEFAULT') = IFNULL(map.psgc_province_name, 'DEFAULT')
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