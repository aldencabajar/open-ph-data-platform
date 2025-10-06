SELECT
    cn.id,
    psgc.barangay_id,
    cn.population,
    cn.census_year,
    cn.source_timestamp_utc
FROM {{ ref('psa_geographical_codes__pivoted') }} AS psgc
LEFT JOIN {{ ref('psa_barangay_census_data') }} AS cn
    ON psgc.barangay_name = cn.barangay
        AND psgc.city_municipality_name = cn.city_municipality
        AND IFNULL(psgc.province_name, '') = IFNULL(cn.province, '')
