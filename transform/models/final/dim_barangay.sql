
WITH province as (
    SELECT 
        id,
        name as province_name,
        geo_code as province_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'PROV'
),

city_municipality as (
    SELECT 
        id,
        name as city_municipality_name,
        geo_code as city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level IN ('CITY', 'MUN')
)


SELECT 
    bg.id,
    p.id as province_id,
    cm.id as city_municipality_id,
    bg.name as barangay_name,
    bg.geo_code,
    bg.urban_rural_class

FROM (
    SELECT *,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 5) || '00000' as deriv_province_geo_code,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 3) || '000' as deriv_city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'BGY' 
) bg
LEFT JOIN province p
ON bg.deriv_province_geo_code = p.province_geo_code
LEFT JOIN city_municipality cm
ON bg.deriv_city_municipality_geo_code = cm.city_municipality_geo_code
