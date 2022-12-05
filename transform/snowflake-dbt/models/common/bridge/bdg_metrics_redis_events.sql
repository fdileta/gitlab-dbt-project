WITH metrics AS (
  SELECT
    *
  FROM {{ ref('dim_ping_metric') }}
),

final AS (
  SELECT
    metrics.metrics_path,
    TRIM(events.value, '"') AS redis_event,
    metrics.data_by_row['options']['aggregate']['operator']::VARCHAR AS aggregate_operator,
    metrics.data_by_row['options']['aggregate']['attribute']::VARCHAR AS aggregate_attribute,
    metrics.metrics_status
  FROM metrics,
    LATERAL FLATTEN(INPUT => PARSE_JSON(data_by_row['options']['events'])) AS events
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-02",
    updated_date="2022-12-02"
) }}
