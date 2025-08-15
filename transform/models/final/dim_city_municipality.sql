
WITH province as (
    SELECT 
        id, 
        name as province_name,
        geo_code as province_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'PROV'
)

SELECT
    cm.id,
    p.id as province_id,
    name as city_municipality_name,
    geo_code,
    area_in_sqm,
    class
FROM (
    SELECT *,
    SUBSTRING(geo_code, 1, LENGTH(geo_code) - 5) || '00000' as deriv_province_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level in ('CITY', 'MUN')
 ) cm
LEFT JOIN province p
ON cm.deriv_province_geo_code = p.province_geo_code
LEFT JOIN {{ ref("wikipedia_city_municipality") }} wcm
ON cm.name = wcm.city_municipality_name AND p.province_name = wcm.province_name







