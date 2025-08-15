
SELECT
{{ dbt_utils.generate_surrogate_key(['geo_code']) }} as id,
name as province_name,
geo_code,
capital,
area_in_sqm,
island_group,
region,
year_founded,
day_of_year_founded
FROM {{ ref('psa_geographical_codes') }}
LEFT JOIN  {{ ref('wikipedia_province_data') }}
ON province_name = name
WHERE geographic_level = 'PROV'