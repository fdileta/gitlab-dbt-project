{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_crm_touchpoint','mart_crm_touchpoint'),
    ('rpt_crm_person_with_opp','rpt_crm_person_with_opp')
]) }}

, base AS (

  SELECT DISTINCT
    mart_crm_touchpoint.dim_crm_person_id,
    mart_crm_touchpoint.email_hash,
    mart_crm_touchpoint.dim_crm_touchpoint_id,
    mart_crm_touchpoint.mql_date_first,
    mart_crm_touchpoint.bizible_touchpoint_date
    FROM mart_crm_touchpoint
    WHERE mql_date_first IS NOT null

), count_of_pre_mql_tps AS (

  SELECT DISTINCT
    email_hash,
    COUNT(DISTINCT dim_crm_touchpoint_id) AS pre_mql_touches
  FROM base
  WHERE bizible_touchpoint_date <= mql_date_first
  GROUP BY 1

), pre_mql_tps_by_person AS (

    SELECT
      email_hash,
      pre_mql_touches,
      1/pre_mql_touches AS pre_mql_weight
    FROM count_of_pre_mql_tps
    GROUP BY 1,2

), pre_mql_tps AS (

    SELECT
      base.dim_crm_touchpoint_id,
      pre_mql_tps_by_person.pre_mql_weight
    FROM pre_mql_tps_by_person
    LEFT JOIN base ON
    pre_mql_tps_by_person.email_hash=base.email_hash
    WHERE bizible_touchpoint_date <= mql_date_first

), post_mql_tps AS (

    SELECT
      base.dim_crm_touchpoint_id,
      0 AS pre_mql_weight
    FROM base
    WHERE bizible_touchpoint_date > mql_date_first
      OR mql_date_first IS null

), mql_weighted_tps AS (

    SELECT *
    FROM pre_mql_tps
    UNION ALL
    SELECT *
    FROM post_mql_tps
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-25",
    updated_date="2022-09-30"
) }}