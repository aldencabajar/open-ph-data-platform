SELECT
    cn.id,
    psgc.barangay_id,
    cn.population,
    cn.census_year,
    cn.source_timestamp_utc
FROM {{ ref('psa_barangay_census_data') }} AS cn
LEFT JOIN {{ ref('psa_geographical_codes__pivoted') }} AS psgc
    ON
        cn.barangay = psgc.barangay_name
        AND cn.city_municipality = psgc.city_municipality_name
        AND cn.province = psgc.province_name
