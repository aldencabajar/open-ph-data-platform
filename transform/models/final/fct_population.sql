
SELECT
    cn.id,
    b.id as barangay_id,
    population,
    census_year,
    source_timestamp_utc
FROM {{ ref('psa_barangay_census_data') }} cn
LEFT JOIN {{ ref('dim_province') }} p
ON cn.province = p.province_name
LEFT JOIN {{ ref('dim_city_municipality')}} cm
ON cn.city_municipality = cm.city_municipality_name
AND p.id = cm.province_id
LEFT JOIN {{ ref('dim_barangay') }} b
ON cn.barangay = b.barangay_name
AND cm.id = b.city_municipality_id
AND p.id = b.province_id



