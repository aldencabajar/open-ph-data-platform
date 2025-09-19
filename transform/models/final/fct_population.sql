
SELECT
    cn.id,
    psgc.barangay_id as barangay_id,
    population,
    census_year,
    source_timestamp_utc
FROM {{ ref('psa_barangay_census_data') }} cn
LEFT JOIN {{ ref('psa_geographical_codes__pivoted') }} as psgc
ON cn.barangay = psgc.barangay_name
AND cn.city_municipality = psgc.city_municipality_name
AND cn.province = psgc.province_name



