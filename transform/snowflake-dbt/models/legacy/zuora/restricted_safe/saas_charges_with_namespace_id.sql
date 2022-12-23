WITH zuora_base_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}
    WHERE delivery = 'SaaS'

)

, customers_db_charges AS (

    SELECT *
    FROM {{ ref('customers_db_charges_xf') }}

)

, namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces') }}

)

, dim_billing_account AS (

    SELECT *
    FROM {{ ref('dim_billing_account') }}

)

, dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

)

, dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

)


, joined AS (

    SELECT
      zuora_base_mrr.rate_plan_charge_id,
      zuora_base_mrr.subscription_name_slugify,
      dim_billing_account.dim_billing_account_id                                        AS dim_billing_account_id,
      COALESCE(merged_accounts.dim_crm_account_id, dim_crm_account.dim_crm_account_id)  AS dim_crm_account_id,
      COALESCE(merged_accounts.dim_parent_crm_account_id,
                dim_crm_account.dim_parent_crm_account_id)                              AS dim_parent_crm_account_id,
      customers_db_charges.current_customer_id,
      namespaces.namespace_id
    FROM zuora_base_mrr
    LEFT JOIN customers_db_charges
      ON zuora_base_mrr.rate_plan_charge_id = customers_db_charges.rate_plan_charge_id
    LEFT JOIN namespaces
      ON customers_db_charges.current_gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN dim_billing_account
      ON zuora_base_mrr.account_number = dim_billing_account.billing_account_number
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_account AS merged_accounts
      ON dim_crm_account.merged_to_account_id = merged_accounts.dim_crm_account_id

)

SELECT *
FROM joined
