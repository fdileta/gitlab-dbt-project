WITH source AS (

  SELECT
    kpi::VARCHAR                            AS kpi,
    month::VARCHAR                          AS month,
    sales_qualified_source::VARCHAR         AS sales_qualified_source,
    order_type_live::VARCHAR                AS order_type_live,
    opportunity_owner_user_area::VARCHAR    AS opportunity_owner_user_area,
    allocated_target                        AS allocated_target,
    fiscal_quarter_name::VARCHAR            AS fiscal_quarter_name,
    fiscal_month::VARCHAR                   AS fiscal_month,
    logos_segment::VARCHAR                  AS logos_segment,
    segment::VARCHAR                        AS segment,
    geo::VARCHAR                            AS geo

  FROM {{ ref('sheetload_fy24_placeholder_target_source') }}

)

SELECT *
FROM source