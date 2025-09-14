-- Test: province_id should NOT be NULL for non-HUCs and non-special geographic areas

WITH special_areas AS (
    SELECT unnest({{ var('special_geographic_areas') }}) AS city_municipality_name
)
SELECT *
FROM {{ ref('dim_city_municipality') }} dcm
LEFT JOIN special_areas sa
  ON dcm.city_municipality_name = sa.city_municipality_name
WHERE (dcm.class != 'HUC' AND sa.city_municipality_name IS NULL)
  AND dcm.province_id IS NULL