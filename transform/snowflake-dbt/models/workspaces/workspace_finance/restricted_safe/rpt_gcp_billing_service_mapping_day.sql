{{ config(
    materialized='table',
    )
}}


WITH export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-01-01'

),

resource_labels as (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}

),

-- RULE DESCRIPTION @ https://docs.google.com/spreadsheets/d/1Fx7Ah2wphM3GoAkPDFBT-89tia3Mgd4BrfWxAj9B-5Q/edit#gid=957450334
-- This query tags each cost line (daily agg) from the billing export with the service tag found @ https://docs.google.com/spreadsheets/d/1UQ2Vuo0wZUtbeSiKhnKpuID-fHTPcQadJslbC2i3KTA/edit#gid=1230668564
  service_coeff as (

  SELECT 1 as relationship_id, 'gitaly networking' as gitlab_service, 0.76 as coeff UNION ALL
  SELECT 1, 'haproxy', 0.24 UNION ALL
  SELECT 2, 'package', 0.5 UNION ALL --122k intersecting on {package, registry storage} so setting 50/50 split TODO: better ssot?
  SELECT 2, 'registry storage', 0.5

  ),
  rule_10_ids AS (
  SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'haproxy' AS service
    FROM
        export
    WHERE
        sku_id = '984A-1F27-2D1F'
        AND export.project_id = 'gitlab-production'
  ),
   rule_9_ids AS (
        with a as (
        SELECT
            DISTINCT export.source_primary_key as source_primary_key
        FROM
            export
        WHERE
            (LOWER(sku_description) like 'download%' or -- `networking` (old) sku mapping
            LOWER(sku_description) like '%egress%' or
            LOWER(sku_description) like 'nat gateway%' or
            LOWER(sku_description) like 'network%' or
            LOWER(sku_description) like '%ip charge%' or

            LOWER(sku_description) like '%pd capacity%' or -- `repo storage` old sku mapping
            LOWER(sku_description) like '%pd snapshot%' or

            LOWER(sku_description) like '%instance core%' or -- `Compute` old mapping
            LOWER(sku_description) like '%instance ram%' or
            LOWER(sku_description) like '%instance%' or
            LOWER(sku_description) like '%cpu%' or
            LOWER(sku_description) like '%core%' or
            LOWER(sku_description) like '%ram%')
            AND project_name = 'gitlab-production'
            AND lower(sku_description) != 'Network Egress via Carrier Peering Network - Americas Based' -- #8
        ),
        b as (
      SELECT resource_labels.source_primary_key
      FROM resource_labels
      WHERE
        resource_labels.resource_label_key = 'pet_name'
        AND resource_labels.resource_label_value ilike '%patroni-%'
        )
    SELECT a.source_primary_key,
        'databases' as service
    FROM a
    JOIN b
    ON a.source_primary_key = b.source_primary_key
  ),
  rule_8_ids AS ( -- ci costs are lower here than in the report because credits were excluded?
    SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'ci' AS service
    FROM
        export
    WHERE
        export.project_name IN (
            'gitlab-ci'
            , 'gitlab-ci-plan-free-4'
            , 'gitlab-ci-plan-free-6'
            , 'gitlab-ci-plan-free-3'
            , 'gitlab-ci-plan-free-5'
            , 'gitlab-ci-plan-free-7'
            , 'gitlab-ci-windows'
            , 'gitlab-org-ci'
            , 'gitlab-qa-ci-load-test'
            , 'gitlab-ci-macos'
            )
  ),
  rule_7_ids AS (
  SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'package' AS service
    FROM
        export
    WHERE
        sku_id in (
        '0D5D-6E23-4250'
        , '37C4-203D-1024'
        , '9E8A-BB82-26BF'
        )
    and export.project_id = 'gitlab-production'
    and export.resource_labels ilike '%package%'
  ),
  rule_6_ids AS (
    SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'lfs' AS service
    FROM
        export
    WHERE
        sku_id in (
        '0D5D-6E23-4250'
        , '37C4-203D-1024'
        , '9E8A-BB82-26BF'
    )
    and
    export.resource_labels ilike '%lfs%'
  ),
  rule_5_ids AS (
    SELECT
        DISTINCT resource_labels.source_primary_key,
        'artifacts' AS service
    FROM
        resource_labels
    WHERE resource_label_value IN (
        'gitlab-gprd-artifacts'
        , 'gitlab-gstg-artifacts'
        , 'gitlab-ops-artifacts'
        , 'gitlab-pre-artifacts'
        , 'gitlab-release-artifacts'
        , 'gitlab-stgsub-artifacts'
        , 'gitlab-testbed-artifacts'
    )
  ),
  rule_4_ids AS (
    SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'registry networking' AS service
    FROM
        export
    WHERE
        lower(export.sku_description) like '%cache%'
  ),
  rule_3_ids AS (
        SELECT
        DISTINCT resource_labels.source_primary_key,
        'registry storage' AS service
    FROM
        resource_labels
    WHERE resource_label_value IN (
        'gitlab-db-benchmarking-registry'
        , 'gitlab-db-sharding-poc-registry'
        , 'gitlab-gprd-container-registry'
        , 'gitlab-gprd-registry'
        , 'gitlab-gstg-container-registry'
        , 'gitlab-gstg-registry'
        , 'gitlab-ops-registry'
        , 'gitlab-pre-container-registry'
        , 'gitlab-pre-registry'
        , 'gitlab-release-registry'
        , 'gitlab-stgsub-registry'
        , 'gitlab-testbed-registry'
    )
  ),
  rule_2_ids AS (
  SELECT
        DISTINCT export.source_primary_key as source_primary_key,
        'gitaly networking' AS service,
        0.76 as coeff
    FROM
        export
    WHERE
        export.sku_id = '984A-1F27-2D1F'
    AND export.project_id = 'gitlab-production'
  ),
  rule_1_ids AS (
    SELECT
        DISTINCT resource_labels.source_primary_key,
        'gitaly compute and storage' AS service
    FROM
        resource_labels
    WHERE
        resource_labels.resource_label_key = 'pet_name'
        AND resource_labels.resource_label_value IN ( 'craig-file-test',
        'file',
        'file-cny',
        'file-hdd',
        'file-marquee',
        'file-praefect',
        'file-zfs',
        'file-zfs-single-zone')
  ),
  billing_base_rules as (
        SELECT
        date(export.usage_start_time) AS day,
        export.project_id AS project_id,
        export.service_description AS service,
        export.sku_description AS sku_description,
        -- case block start
        CASE
            WHEN rule_10_ids.source_primary_key = export.source_primary_key THEN rule_10_ids.service
        END
        AS rule_10,
        -- case block start
        CASE
            WHEN rule_9_ids.source_primary_key = export.source_primary_key THEN rule_9_ids.service
        END
        AS rule_9,
        -- case block start
        CASE
            WHEN rule_8_ids.source_primary_key = export.source_primary_key THEN rule_8_ids.service
        END
        AS rule_8,
        -- case block start
        CASE
            WHEN rule_7_ids.source_primary_key = export.source_primary_key THEN rule_7_ids.service
        END
        AS rule_7,
        -- case block start
        CASE
            WHEN rule_6_ids.source_primary_key = export.source_primary_key THEN rule_6_ids.service
        END
        AS rule_6,
        -- case block start
        CASE
            WHEN rule_5_ids.source_primary_key = export.source_primary_key THEN rule_5_ids.service
        END
        AS rule_5,
        -- case block start
        CASE
            WHEN rule_4_ids.source_primary_key = export.source_primary_key THEN rule_4_ids.service
        END
        AS rule_4,
        -- case block start
        CASE
            WHEN rule_3_ids.source_primary_key = export.source_primary_key THEN rule_3_ids.service
        END
        AS rule_3,
        -- case block start
        CASE
            WHEN rule_2_ids.source_primary_key = export.source_primary_key THEN rule_2_ids.service
        END
        AS rule_2,
        -- case block start
        CASE
            WHEN rule_1_ids.source_primary_key = export.source_primary_key THEN rule_1_ids.service
        END
        AS rule_1,
        -- net costs
        sum(export.total_cost) AS net_cost
        FROM
        export

        -- join block start
        LEFT JOIN
        rule_10_ids
        ON
        export.source_primary_key = rule_10_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_9_ids
        ON
        export.source_primary_key = rule_9_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_8_ids
        ON
        export.source_primary_key = rule_8_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_7_ids
        ON
        export.source_primary_key = rule_7_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_6_ids
        ON
        export.source_primary_key = rule_6_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_5_ids
        ON
        export.source_primary_key = rule_5_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_4_ids
        ON
        export.source_primary_key = rule_4_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_3_ids
        ON
        export.source_primary_key = rule_3_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_2_ids
        ON
        export.source_primary_key = rule_2_ids.source_primary_key
        -- join block start
        LEFT JOIN
        rule_1_ids
        ON
        export.source_primary_key = rule_1_ids.source_primary_key
        -- -- -- -- -- -- 
        WHERE
        invoice_month = '2022-11-01'
       {{ dbt_utils.group_by(n=14) }})
SELECT
        billing_base_rules.day as day,
        billing_base_rules.project_id as gcp_project_id,
        billing_base_rules.service as gcp_service_description,
        billing_base_rules.sku_description as gcp_sku_description,
        coalesce(service_coeff.gitlab_service, concat(coalesce(billing_base_rules.rule_1, ''),
                                                            coalesce(billing_base_rules.rule_2, ''),
                                                            coalesce(billing_base_rules.rule_3, ''),
                                                            coalesce(billing_base_rules.rule_4, ''),
                                                            coalesce(billing_base_rules.rule_5, ''),
                                                            coalesce(billing_base_rules.rule_6, ''),
                                                            coalesce(billing_base_rules.rule_7, ''),
                                                            coalesce(billing_base_rules.rule_8, ''),
                                                            coalesce(billing_base_rules.rule_9, ''),
                                                            coalesce(billing_base_rules.rule_10, ''))) as gitlab_service,
        sum(billing_base_rules.net_cost) * ifnull(service_coeff.coeff, 1) as net_cost
FROM billing_base_rules LEFT JOIN service_coeff
        on billing_base_rules.rule_1 = service_coeff.gitlab_service
        or billing_base_rules.rule_2 = service_coeff.gitlab_service
        or billing_base_rules.rule_3 = service_coeff.gitlab_service
        or billing_base_rules.rule_4 = service_coeff.gitlab_service
        or billing_base_rules.rule_5 = service_coeff.gitlab_service
        or billing_base_rules.rule_6 = service_coeff.gitlab_service
        or billing_base_rules.rule_7 = service_coeff.gitlab_service
        or billing_base_rules.rule_8 = service_coeff.gitlab_service
        or billing_base_rules.rule_9 = service_coeff.gitlab_service
        or billing_base_rules.rule_10 = service_coeff.gitlab_service
group by 1, 2, 3, 4, 5, service_coeff.coeff
