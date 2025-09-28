WITH province_city_municipality_geo AS (
    SELECT DISTINCT
        psgc.city_municipality_id,
        COALESCE(map.wikipedia_province_name, psgc.province_name)
            AS province_name,
        COALESCE(map.wikipedia_city_mun_name, psgc.city_municipality_name)
            AS city_municipality_name
    FROM {{ ref('psa_geographical_codes__pivoted') }} AS psgc
    LEFT JOIN {{ ref('psgc_wikipedia_city_mun_mapping') }} AS map
        ON
            psgc.city_municipality_name = map.psgc_city_mun_name
            AND COALESCE(psgc.province_name, 'DEFAULT')
            = COALESCE(map.psgc_province_name, 'DEFAULT')
)

SELECT
    wcm.*,
    pcmg.city_municipality_id
FROM {{ ref('wikipedia_city_municipality') }} AS wcm
LEFT JOIN province_city_municipality_geo AS pcmg
    ON
        {{ normalize_city_name('wcm.city_municipality_name') }}
        = {{ normalize_city_name('pcmg.city_municipality_name') }}
        AND COALESCE(wcm.province_name, 'DEFAULT')
        = COALESCE(pcmg.province_name, 'DEFAULT')
