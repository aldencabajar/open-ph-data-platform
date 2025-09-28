SELECT
    barangay_id AS id,
    city_municipality_id,
    barangay_name,
    barangay_geo_code AS geo_code,
    urban_rural_class
FROM {{ ref('psa_geographical_codes__pivoted') }}
