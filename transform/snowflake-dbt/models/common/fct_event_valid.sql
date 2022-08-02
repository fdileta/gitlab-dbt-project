{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event', 'fct_event'),
    ('dim_user', 'dim_user'),
    ('xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])
}},

fct_event_valid AS (
    
    /*
    fct_event_valid is at the atomic grain of event_id and event_created_at timestamp. All other derived facts in the GitLab.com usage events 
    lineage are built from this derived fact. This CTE pulls in ALL of the columns from the fct_event as a base data set. It uses the dbt_utils.star function 
    to select all columns except the meta data table related columns from the fct_event. The CTE also filters out imported projects and events with 
    data quality issues by filtering out negative days since user creation at event date. It keeps events with a NULL days since user creation to capture events
    that do not have a user. fct_event_valid also filters out events from blocked users with a join back to dim_user. The table also filters to a rolling 24 months of data 
    for performance optimization.
    */

    SELECT
      fct_event.dim_user_sk,
      fct_event.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
      {{ dbt_utils.star(from=ref('fct_event'), except=["DIM_USER_SK", "DIM_USER_ID", "CREATED_BY",
          "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }},
      xmau_metrics.group_name,
      xmau_metrics.section_name,
      xmau_metrics.stage_name,
      xmau_metrics.smau AS is_smau,
      xmau_metrics.gmau AS is_gmau,
      xmau_metrics.is_umau
    FROM fct_event
    LEFT JOIN xmau_metrics
      ON fct_event.event_name = xmau_metrics.common_events_to_include
    LEFT JOIN dim_user
      ON fct_event.dim_user_sk = dim_user.dim_user_sk
    WHERE DATE_TRUNC(MONTH,fct_event.event_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE)) 
      AND (fct_event.days_since_user_creation_at_event_date >= 0
           OR fct_event.days_since_user_creation_at_event_date IS NULL)
      AND (dim_user.is_blocked_user = FALSE 
           OR dim_user.is_blocked_user IS NULL)

),

deduped_namespace_bdg AS (

  SELECT
    namespace_order_subscription.dim_subscription_id AS dim_latest_subscription_id,
    namespace_order_subscription.order_id,
    namespace_order_subscription.dim_crm_account_id,
    namespace_order_subscription.dim_billing_account_id,
    namespace_order_subscription.dim_namespace_id
  FROM namespace_order_subscription
  INNER JOIN dim_subscription
    ON namespace_order_subscription.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE namespace_order_subscription.product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY subscription_version DESC) = 1

),

dim_namespace_w_bdg AS (

  SELECT
    dim_namespace.dim_namespace_id,
    dim_namespace.dim_product_tier_id AS dim_active_product_tier_id,
    deduped_namespace_bdg.dim_latest_subscription_id,
    deduped_namespace_bdg.order_id,
    deduped_namespace_bdg.dim_crm_account_id,
    deduped_namespace_bdg.dim_billing_account_id
  FROM deduped_namespace_bdg
  INNER JOIN dim_namespace
    ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

),

paid_flag_by_day AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    plan_was_paid_at_event_timestamp AS plan_was_paid_at_event_date,
    plan_id_at_event_timestamp AS plan_id_at_event_date,
    plan_name_at_event_timestamp AS plan_name_at_event_date,
    event_created_at,
    event_date
  FROM fct_event_valid
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_date
      ORDER BY event_created_at DESC) = 1

),

fct_event_w_flags AS (

  SELECT 
    fct_event_valid.event_pk,
    fct_event_valid.dim_event_date_id,
    fct_event_valid.dim_ultimate_parent_namespace_id,
    fct_event_valid.dim_project_id,
    fct_event_valid.dim_user_sk,
    fct_event_valid.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be deprecated in a future MR.
    fct_event_valid.is_null_user,
    fct_event_valid.event_created_at,
    fct_event_valid.event_date,
    fct_event_valid.group_name,
    fct_event_valid.section_name,
    fct_event_valid.stage_name,
    fct_event_valid.is_smau,
    fct_event_valid.is_gmau,
    fct_event_valid.is_umau,
    fct_event_valid.parent_id,
    fct_event_valid.parent_type,
    fct_event_valid.event_name,
    fct_event_valid.days_since_user_creation_at_event_date,
    fct_event_valid.days_since_namespace_creation_at_event_date,
    fct_event_valid.days_since_project_creation_at_event_date,
    fct_event_valid.data_source,
    dim_namespace_w_bdg.dim_active_product_tier_id,
    dim_namespace_w_bdg.dim_latest_subscription_id,
    dim_namespace_w_bdg.order_id,
    dim_namespace_w_bdg.dim_crm_account_id,
    dim_namespace_w_bdg.dim_billing_account_id,
    COALESCE(paid_flag_by_day.plan_was_paid_at_event_date, FALSE) AS plan_was_paid_at_event_date,
    COALESCE(paid_flag_by_day.plan_id_at_event_date, 34) AS plan_id_at_event_date,
    COALESCE(paid_flag_by_day.plan_name_at_event_date, 'free') AS plan_name_at_event_date
  FROM fct_event_valid
  LEFT JOIN dim_namespace_w_bdg
    ON fct_event_valid.dim_ultimate_parent_namespace_id = dim_namespace_w_bdg.dim_namespace_id
  LEFT JOIN paid_flag_by_day
    ON fct_event_valid.dim_ultimate_parent_namespace_id = paid_flag_by_day.dim_ultimate_parent_namespace_id
      AND fct_event_valid.event_date = paid_flag_by_day.event_date

),

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    event_pk,
    
    --Foreign Keys
    dim_event_date_id,
    dim_ultimate_parent_namespace_id,
    dim_project_id,
    dim_user_sk,
    dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
    dim_active_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    order_id,
    
    --Time attributes
    event_created_at,
    event_date,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    is_null_user,
    group_name,
    section_name,
    stage_name,
    is_smau,
    is_gmau,
    is_umau,
    parent_id,
    parent_type,
    event_name,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date,
    days_since_user_creation_at_event_date,
    days_since_namespace_creation_at_event_date,
    days_since_project_creation_at_event_date,
    data_source
  FROM fct_event_w_flags

)

{{ dbt_audit(
    cte_ref="gitlab_dotcom_fact",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-06-20"
) }}
