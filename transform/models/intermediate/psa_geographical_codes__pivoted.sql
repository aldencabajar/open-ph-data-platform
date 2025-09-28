WITH province AS (
    SELECT
        id AS province_id,
        name AS province_name,
        geo_code AS province_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'PROV'
),

city_municipality AS (
    SELECT
        id AS city_municipality_id,
        name AS city_municipality_name,
        geo_code AS city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level IN ('CITY', 'MUN')
),

/*
This is a special case for Manila since it is divided first
into districts (sub-municipalities)
*/

submun AS (
    SELECT
        sm.id AS sub_municipality_id,
        sm.name AS sub_municipality_name,
        sm.geo_code AS sub_municipality_geo_code,
        cm.city_municipality_id,
        cm.city_municipality_name,
        cm.city_municipality_geo_code
    FROM (
        SELECT
            *,
            SUBSTRING(geo_code, 1, LENGTH(geo_code) - 5)
            || '00000' AS deriv_city_municipality_geo_code
        FROM {{ ref('psa_geographical_codes') }}
    ) AS sm
    LEFT JOIN city_municipality AS cm
        ON sm.deriv_city_municipality_geo_code = cm.city_municipality_geo_code
    WHERE sm.geographic_level = 'SUBMUN'
),

brgy AS (
    SELECT
        *,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 5)
        || '00000' AS deriv_province_geo_code,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 3)
        || '000' AS deriv_submun_geo_code,
        SUBSTRING(geo_code, 1, LENGTH(geo_code) - 3)
        || '000' AS deriv_city_municipality_geo_code
    FROM {{ ref('psa_geographical_codes') }}
    WHERE geographic_level = 'BGY'
)

SELECT
    p.province_id,
    p.province_name,
    p.province_geo_code,
    sm.sub_municipality_id,
    sm.sub_municipality_name,
    sm.sub_municipality_geo_code,
    brgy.id AS barangay_id,
    brgy.name AS barangay_name,
    brgy.geo_code AS barangay_geo_code,
    brgy.urban_rural_class,
    COALESCE(cm.city_municipality_id, sm.city_municipality_id)
        AS city_municipality_id,
    COALESCE(cm.city_municipality_name, sm.city_municipality_name)
        AS city_municipality_name,
    COALESCE(cm.city_municipality_geo_code, sm.city_municipality_geo_code)
        AS city_municipality_geo_code
FROM brgy
LEFT JOIN province AS p
    ON brgy.deriv_province_geo_code = p.province_geo_code
LEFT JOIN city_municipality AS cm
    ON brgy.deriv_city_municipality_geo_code = cm.city_municipality_geo_code
LEFT JOIN submun AS sm
    ON brgy.deriv_submun_geo_code = sm.sub_municipality_geo_code
