WITH province as (
    SELECT 
        id as province_id,
        name as province_name,
        geo_code as province_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'PROV'
),

city_municipality as (
    SELECT 
        id as city_municipality_id,
        name as city_municipality_name,
        geo_code as city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level IN ('CITY', 'MUN')
),

brgy as (
    SELECT *,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 5) || '00000' as deriv_province_geo_code,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 3) || '000' as deriv_city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'BGY' 
)


SELECT 
    province_id,
    province_name,
    province_geo_code,
    city_municipality_id,
    city_municipality_name,
    city_municipality_geo_code,
    brgy.id as barangay_id,
    brgy.name as barangay_name,
    brgy.geo_code as barangay_geo_code,
    brgy.urban_rural_class
FROM brgy
LEFT JOIN province p
ON brgy.deriv_province_geo_code = p.province_geo_code
LEFT JOIN city_municipality cm
ON brgy.deriv_city_municipality_geo_code = cm.city_municipality_geo_code

