{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account')
]) }}

, prep_charge AS (

    SELECT *
    FROM {{ ref('prep_charge') }}
    WHERE is_manual_charge = 0

), mrr AS (

    SELECT
      md5(cast(coalesce(cast(dim_date.date_id as varchar), '') || '-' || coalesce(cast(prep_charge.dim_charge_id as varchar), '') as varchar))       AS mrr_id,
      dim_date.date_id                                                                      AS dim_date_id,
      prep_charge.dim_charge_id,
      prep_charge.dim_product_detail_id,
      prep_charge.dim_subscription_id,
      prep_charge.dim_billing_account_id,
      prep_charge.dim_crm_account_id,
      prep_charge.subscription_status,
      prep_charge.unit_of_measure,
      SUM(prep_charge.mrr)                                                                  AS mrr,
      SUM(prep_charge.arr)                                                                  AS arr,
      SUM(prep_charge.quantity)                                                             AS quantity
    FROM prep_charge
    INNER JOIN dim_date
      ON prep_charge.effective_start_month <= dim_date.date_actual
      AND (prep_charge.effective_end_month > dim_date.date_actual
        OR prep_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    WHERE subscription_status != 'Draft'
      AND charge_type = 'Recurring'
      /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
         Pull in seats from Paid EDU Plans with no ARR */
      AND (mrr != 0 OR LOWER(prep_charge.rate_plan_charge_name) = 'max enrollment')
    {{ dbt_utils.group_by(n=9) }}

), fct_mrr AS (

  SELECT
    dim_date_id,
    dim_subscription_id,
    dim_product_detail_id,
    dim_billing_account_id,
    dim_crm_account_id,
    SUM(mrr)                                                                      AS mrr,
    SUM(arr)                                                                      AS arr,
    SUM(quantity)                                                                 AS quantity,
    ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure)   AS unit_of_measure
  FROM mrr
  WHERE subscription_status IN ('Active', 'Cancelled')
  {{ dbt_utils.group_by(n=5) }}

), joined AS (

    SELECT
      --primary_key
      {{ dbt_utils.surrogate_key(['fct_mrr.dim_date_id', 'dim_subscription.subscription_name', 'fct_mrr.dim_product_detail_id']) }} AS primary_key,

      --date info
      dim_date.date_actual                                                            AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS fiscal_year,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_billing_account.ssp_channel                                                 AS ssp_channel,
      dim_billing_account.po_required                                                 AS po_required,
      dim_billing_account.auto_pay                                                    AS auto_pay,
      dim_billing_account.default_payment_method_type                                 AS default_payment_method_type,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_demographics_max_family_employee             AS parent_crm_account_max_family_employees,
      dim_crm_account.crm_account_tsp_region                                          AS crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region                                      AS crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area                                            AS crm_account_tsp_area,
      dim_crm_account.health_score                                                    AS health_score,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,
      dim_crm_account.is_jihu_account                                                 AS is_jihu_account,
      dim_crm_account.parent_crm_account_lam                                          AS parent_crm_account_lam,
      dim_crm_account.parent_crm_account_lam_dev_count                                AS parent_crm_account_lam_dev_count,
      dim_crm_account.parent_crm_account_demographics_sales_segment                   AS parent_crm_account_demographics_sales_segment,
      dim_crm_account.parent_crm_account_demographics_geo                             AS parent_crm_account_demographics_geo,
      dim_crm_account.parent_crm_account_demographics_region                          AS parent_crm_account_demographics_region,
      dim_crm_account.parent_crm_account_demographics_area                            AS parent_crm_account_demographics_area,
      dim_crm_account.parent_crm_account_demographics_territory                       AS parent_crm_account_demographics_territory,


      --subscription info
      dim_subscription.dim_subscription_id                                            AS dim_subscription_id,
      dim_subscription.dim_subscription_id_original                                   AS dim_subscription_id_original,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name                                              AS subscription_name,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_quarter,
      dim_subscription.auto_renew_native_hist,
      dim_subscription.auto_renew_customerdot_hist,
      dim_subscription.turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics,
      dim_subscription.contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation,
      dim_subscription.invoice_owner_account,
      dim_subscription.creator_account,
      dim_subscription.was_purchased_through_reseller,

      --product info
      dim_product_detail.dim_product_detail_id                                        AS dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.product_ranking                                              AS product_ranking,
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,
      dim_product_detail.is_licensed_user                                             AS is_licensed_user,
      dim_product_detail.is_arpu                                                      AS is_arpu,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr.unit_of_measure                                                         AS unit_of_measure,
      fct_mrr.mrr                                                                     AS mrr,
      fct_mrr.arr                                                                     AS arr,
      fct_mrr.quantity                                                                AS quantity
    FROM fct_mrr
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE dim_crm_account.is_jihu_account != 'TRUE'

), cohort_diffs AS (

  SELECT
    joined.*,
    DATEDIFF(month, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    DATEDIFF(quarter, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    DATEDIFF(month, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    DATEDIFF(quarter, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    DATEDIFF(month, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    DATEDIFF(quarter, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    DATEDIFF(month, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    DATEDIFF(quarter, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined

), parent_arr AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                   AS arr
    FROM joined
    group by 1,2

), parent_arr_band_calc AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                        AS arr_band_calc
    FROM parent_arr

), final AS (

    SELECT
      cohort_diffs.*,
      arr_band_calc
    FROM cohort_diffs
    LEFT JOIN parent_arr_band_calc
      ON cohort_diffs.arr_month = parent_arr_band_calc.arr_month
      AND cohort_diffs.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id

)
{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-11-18",
    updated_date="2022-11-18",
) }}
