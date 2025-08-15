
SELECT *
FROM {{ ref('wikipedia_province_data') }}
WHERE day_of_year_founded is not null AND 
      (day_of_year_founded < 1 OR day_of_year_founded > 366)