WITH my_data AS (

  SELECT *
  FROM prod.my_data
  WHERE filter = 'my_filter'

),

some_cte AS (

  SELECT DISTINCT
    id                                                       AS other_id,
    other_field_1,
    other_field_2,
    date_field_at,
    data_by_row,
    field_4,
    field_5,
    LAG(
      other_field_2
    ) OVER (PARTITION BY other_id, other_field_1 ORDER BY 5) AS previous_other_field_2
  FROM prod.my_other_data

),
/*
This is a very long comment: It is good practice to leave comments in code to
explain complex logic in CTEs or business logic which may not be intuitive to
someone who does not have intimate knowledge of the data source. This can help
new users familiarize themselves with the code quickly.
*/

final AS (

  SELECT
    -- This is a singel line comment
    my_data.field_1                                              AS detailed_field_1,
    my_data.field_2                                              AS detailed_field_2,
    my_data.detailed_field_3,
    DATE_TRUNC('month', some_cte.date_field_at)                  AS date_field_month,
    some_cte.data_by_row['id']::NUMBER                           AS id_field,
    IFF(my_data.detailed_field_3 > my_data.field_2, TRUE, FALSE) AS is_boolian,
    CASE
      WHEN my_data.cancellation_date IS NULL
        AND my_data.expiration_date IS NOT NULL
        THEN my_data.expiration_date
      WHEN my_data.cancellation_date IS NULL
        THEN my_data.start_date + 7 -- There is a reason for this number
      ELSE my_data.cancellation_date
    END                                                          AS adjusted_cancellation_date,
    SUM(some_cte.field_4)                                        AS field_4_sum,
    MAX(some_cte.field_5)                                        AS field_5_max
  FROM my_data
  LEFT JOIN some_cte
    ON my_data.id = some_cte.id
  WHERE my_data.field_1 = 'abc'
    AND (my_data.field_2 = 'def' OR my_data.field_2 = 'ghi')
  GROUP BY 1, 2, 3, 4, 5, 6
  HAVING COUNT(*) > 1
  ORDER BY 8 DESC
)

SELECT *
FROM final
