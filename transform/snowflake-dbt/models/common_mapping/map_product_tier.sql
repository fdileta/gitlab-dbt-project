WITH zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      zuora_product_rate_plan.product_rate_plan_id                  AS product_rate_plan_id,
      zuora_product_rate_plan.product_rate_plan_name                AS product_rate_plan_name,
      CASE
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%saas - ultimate%'
          THEN 'SaaS - Ultimate'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%saas - premium%'
          THEN 'SaaS - Premium'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%dedicated - ultimate%'
          THEN 'Dedicated - Ultimate'        
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%ultimate%'
          THEN 'Self-Managed - Ultimate'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%premium%'
          THEN 'Self-Managed - Premium'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'gold%'
          THEN 'SaaS - Gold'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'silver%'
          THEN 'SaaS - Silver'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%bronze%'
          THEN 'SaaS - Bronze'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%starter%'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'gitlab enterprise edition%'
          THEN 'Self-Managed - Starter'
        WHEN zuora_product_rate_plan.product_rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'plus%'
          THEN 'Plus'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'standard%'
          THEN 'Standard'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'basic%'
          THEN 'Basic'
        WHEN zuora_product_rate_plan.product_rate_plan_name = 'Trueup'
          THEN 'Trueup'
        WHEN LTRIM(LOWER(zuora_product_rate_plan.product_rate_plan_name)) LIKE 'githost%'
          THEN 'GitHost'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE ANY ('%quick start with ha%', '%proserv training per-seat add-on%', 'dedicated engineer%')
          THEN 'Support'
        WHEN TRIM(zuora_product_rate_plan.product_rate_plan_name) IN (
                                                                        'GitLab Service Package'
                                                                      , 'Implementation Services Quick Start'
                                                                      , 'Implementation Support'
                                                                      , 'Support Package'
                                                                      , 'Admin Training'
                                                                      , 'CI/CD Training'
                                                                      , 'GitLab Project Management Training'
                                                                      , 'GitLab with Git Basics Training'
                                                                      , 'Travel Expenses'
                                                                      , 'Training Workshop'
                                                                      , 'GitLab for Project Managers Training - Remote'
                                                                      , 'GitLab with Git Basics Training - Remote'
                                                                      , 'GitLab for System Administrators Training - Remote'
                                                                      , 'GitLab CI/CD Training - Remote'
                                                                      , 'InnerSourcing Training - Remote for your team'
                                                                      , 'GitLab DevOps Fundamentals Training'
                                                                      , 'Self-Managed Rapid Results Consulting'
                                                                      , 'Gitlab.com Rapid Results Consulting'
                                                                      , 'GitLab Security Essentials Training - Remote Delivery'
                                                                      , 'InnerSourcing Training - At your site'
                                                                      , 'Migration+'
                                                                      , 'One Time Discount'
                                                                      , 'LDAP Integration'
                                                                      , 'Dedicated Implementation Services'
                                                                      , 'Quick Start without HA, less than 500 users'
                                                                      , 'Jenkins Integration'
                                                                      , 'Hourly Consulting'
                                                                      , 'JIRA Integration'
                                                                      , 'Custom PS Education Services'
                                                                     )
          THEN 'Support'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'gitlab geo%'
          THEN 'SaaS - Other'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'ci runner%'
          THEN 'SaaS - Other'
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE 'discount%'
          THEN 'Other'
        WHEN TRIM(zuora_product_rate_plan.product_rate_plan_name) IN (
                                                                        '#movingtogitlab'
                                                                      , 'Payment Gateway Test'
                                                                      , 'EdCast Settlement Revenue'
                                                                     )
          THEN 'Other'
        WHEN TRIM(zuora_product_rate_plan.product_rate_plan_name) IN (
                                                                        'File Locking'
                                                                      , 'Time Tracking'
                                                                      , '1,000 CI Minutes'
                                                                     )
          THEN 'SaaS - Other'
        WHEN TRIM(zuora_product_rate_plan.product_rate_plan_name) IN ('Gitlab Storage 10GB')
          THEN 'Storage'
        ELSE 'Not Applicable'
      END                                                           AS product_tier_historical,
      CASE
        WHEN LOWER(product_tier_historical) LIKE '%self-managed%'
          THEN 'Self-Managed'
        WHEN LOWER(product_tier_historical) LIKE ANY ('%saas%', 'storage', 'standard', 'basic', 'plus', 'githost', 'dedicated')
          THEN 'SaaS'
        WHEN product_tier_historical = 'SaaS - Other'
          THEN 'SaaS'
        WHEN product_tier_historical IN (
                                          'Other'
                                        , 'Support'
                                        , 'Trueup'
                                        )
          THEN 'Others'
        ELSE NULL
      END                                                           AS product_delivery_type,
      CASE
        WHEN product_tier_historical IN (
                                          'SaaS - Gold'
                                        , 'Self-Managed - Ultimate'
                                        , 'SaaS - Ultimate'
                                        , 'Dedicated'
                                        )
          THEN 3
        WHEN product_tier_historical IN (
                                          'SaaS - Silver'
                                        , 'Self-Managed - Premium'
                                        , 'SaaS - Premium'
                                        )
          THEN 2
        WHEN product_tier_historical IN (
                                          'SaaS - Bronze'
                                        , 'Self-Managed - Starter'
                                        )
          THEN 1
        ELSE 0
      END                                                           AS product_ranking,
      CASE
        WHEN product_tier_historical = 'SaaS - Gold'
          THEN 'SaaS - Ultimate'
        WHEN product_tier_historical = 'SaaS - Silver'
          THEN 'SaaS - Premium'
        ELSE product_tier_historical
      END                                                           AS product_tier
    FROM zuora_product
    INNER JOIN zuora_product_rate_plan
      ON zuora_product.product_id = zuora_product_rate_plan.product_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@chrissharp",
    created_date="2020-12-14",
    updated_date="2022-12-05"
) }}
