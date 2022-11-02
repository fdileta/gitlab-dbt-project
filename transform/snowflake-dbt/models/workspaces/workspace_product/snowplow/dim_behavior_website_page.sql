{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_website_page_sk"
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all')
    ])
}}

, page_url AS (

    SELECT
      app_id,
      page_urlpath                                                                  AS page_url_path,
      {{ clean_url('page_urlpath') }}                                               AS clean_url_path,
      page_urlhost                                                                  AS page_url_host,
      page_urlscheme                                                                AS page_url_scheme,
      page_urlfragment                                                              AS page_url_fragment,
      SPLIT_PART(clean_url_path, '/' ,1)                                            AS page_group,
      SPLIT_PART(clean_url_path, '/' ,2)                                            AS page_type,
      SPLIT_PART(clean_url_path, '/' ,3)                                            AS page_sub_type,
      refr_medium                                                                   AS referrer_medium,
      min(uploaded_at)                                                              AS min_event_timestamp,
      max(uploaded_at)                                                              AS max_event_timestamp
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')
    AND page_urlpath IS NOT NULL

    {% if is_incremental() %}

    AND uploaded_at > (SELECT max(max_event_timestamp) FROM {{ this }})

    {% endif %}

    {{ dbt_utils.group_by(n=11) }}

), referer_url AS (

    SELECT
      app_id,
      refr_urlpath                                                                  AS page_url_path,
      {{ clean_url('refr_urlpath') }}                                               AS clean_url_path,
      refr_urlhost                                                                  AS page_url_host,
      refr_urlscheme                                                                AS page_url_scheme,
      refr_urlfragment                                                              AS page_url_fragment,
      SPLIT_PART(clean_url_path, '/' ,1)                                            AS page_group,
      SPLIT_PART(clean_url_path, '/' ,2)                                            AS page_type,
      SPLIT_PART(clean_url_path, '/' ,3)                                            AS page_sub_type,
      refr_medium                                                                   AS referrer_medium,
      min(uploaded_at)                                                              AS min_event_timestamp,
      max(uploaded_at)                                                              AS max_event_timestamp
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')
    AND refr_urlpath IS NOT NULL

    {% if is_incremental() %}

    AND uploaded_at > (SELECT max(max_event_timestamp) FROM {{ this }})

    {% endif %}

    {{ dbt_utils.group_by(n=11) }}

), page AS (

    SELECT *
    FROM page_url

    UNION

    SELECT *
    FROM referer_url

), dim_with_sk AS (

    SELECT DISTINCT
      -- Surrogate Key
      {{ dbt_utils.surrogate_key(['page_url_path', 'app_id']) }}               AS dim_behavior_website_page_sk,

      -- Natural Keys
      page_url_path,
      app_id,

      -- Attributes
      clean_url_path,
      page_url_host,
      page_url_scheme,
      page_url_fragment,
      page_group,
      page_type,
      page_sub_type,
      referrer_medium,
      min_event_timestamp,
      max_event_timestamp,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1) AS url_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1) AS url_project_id,
      CASE 
        WHEN page_url_path LIKE '%/activity' 
          THEN 'Project information - Activity'
        WHEN page_url_path LIKE '%/-/labels' 
          THEN 'Project information - Labels'
        WHEN page_url_path LIKE '%/-/project_members' 
          THEN 'Project information - Members'
        WHEN page_url_path LIKE '%/-/tree/main' 
          THEN 'Repository - Files'
        WHEN page_url_path LIKE '%/-/commits/main' 
          THEN 'Repository - Commits'
        WHEN page_url_path LIKE '%/-/branches' 
          THEN 'Repository - Branches'
        WHEN page_url_path LIKE '%/-/tags' 
          THEN 'Repository - Tags'
        WHEN page_url_path LIKE '%/-/graphs/main' 
          THEN 'Repository - Contributors'
        WHEN page_url_path LIKE '%/-/network/main' 
          THEN 'Repository - Graph'
        WHEN page_url_path LIKE '%/-/compare' 
          THEN 'Repository - Compare'
        WHEN page_url_path LIKE '%/path_locks' 
          THEN 'Repository - Locked Files'
        WHEN page_url_path LIKE '%/-/issues/service_desk' 
          THEN 'Issues - Service Desk'
        WHEN page_url_path LIKE '%/-/issues' 
          THEN 'Issues - List'
        WHEN page_url_path LIKE '%/-/boards' 
          THEN 'Issues - Boards'
        WHEN page_url_path LIKE '%/-/milestones' 
          THEN 'Issues - Milestones'
        WHEN page_url_path LIKE '%/-/iterations' 
          THEN 'Issues - Iterations'
        WHEN page_url_path LIKE '%/-/requirements_management/requirements' 
          THEN 'Issues - Requirements'
        WHEN page_url_path LIKE '%/-/merge_requests' 
          THEN 'Merge requests'
        WHEN page_url_path LIKE '%/-/pipelines' 
          THEN 'CI/CD - Pipelines'
        WHEN page_url_path LIKE '%/-/ci/editor' 
          THEN 'CI/CD - Editor'
        WHEN page_url_path LIKE '%/-/jobs' 
          THEN 'CI/CD - Jobs'
        WHEN page_url_path LIKE '%/-/pipeline_schedules' 
          THEN 'CI/CD - Schedules'
        WHEN page_url_path LIKE '%/-/quality/test_cases' 
          THEN 'CI/CD - Test Cases'
        WHEN page_url_path LIKE '%/-/security/dashboard' 
          THEN 'Security & Compliance - Security dashboard'
        WHEN page_url_path LIKE '%/-/security/vulnerability_report' 
          THEN 'Security & Compliance - Vulnerability report'
        WHEN page_url_path LIKE '%/-/on_demand_scans' 
          THEN 'Security & Compliance - On-demand scans'
        WHEN page_url_path LIKE '%/-/dependencies' 
          THEN 'Security & Compliance - Dependency list'
        WHEN page_url_path LIKE '%/-/licenses' 
          THEN 'Security & Compliance - License compliance'
        WHEN page_url_path LIKE '%/-/security/policies' 
          THEN 'Security & Compliance - Policies'
        WHEN page_url_path LIKE '%/-/audit_events' 
          THEN 'Security & Compliance - Audit events'
        WHEN page_url_path LIKE '%/-/security/configuration' 
          THEN 'Security & Compliance - Configuration'
        WHEN page_url_path LIKE '%/-/feature_flags' 
          THEN 'Deployments - Feature Flags'
        WHEN page_url_path LIKE '%/-/environments' 
          THEN 'Deployments - Environments'
        WHEN page_url_path LIKE '%/-/releases' 
          THEN 'Deployments - Releases'
        WHEN page_url_path LIKE '%/-/metrics' 
          THEN 'Monitor - Metrics'
        WHEN page_url_path LIKE '%/-/logs' 
          THEN 'Monitor - Logs'
        WHEN page_url_path LIKE '%/-/error_tracking' 
          THEN 'Monitor - Error Tracking'
        WHEN page_url_path LIKE '%/-/alert_management' 
          THEN 'Monitor - Alerts'
        WHEN page_url_path LIKE '%/-/incidents' 
          THEN 'Monitor - Incidents'
        WHEN page_url_path LIKE '%/-/oncall_schedules' 
          THEN 'Monitor - On-call Schedules'
        WHEN page_url_path LIKE '%/-/escalation_policies' 
          THEN 'Monitor - Escalation Policies'
        WHEN page_url_path LIKE '%/-/clusters' 
          THEN 'Infrastructure - Kubernetes clusters'
        WHEN page_url_path LIKE '%/-/serverless/functions' 
          THEN 'Infrastructure - Serverless platform'
        WHEN page_url_path LIKE '%/-/terraform' 
          THEN 'Infrastructure - Terraform'
        WHEN page_url_path LIKE '%/-/packages' 
          THEN 'Packages & Registries - Package Registry'
        WHEN page_url_path LIKE '%/container_registry' 
          THEN 'Packages & Registries - Container Registry'
        WHEN page_url_path LIKE '%/-/infastructure_registry' 
          THEN 'Packages & Registries - Infastructure Registry'
        WHEN page_url_path LIKE '%/-/value_stream_analytics' 
          THEN 'Analytics - Value stream'
        WHEN page_url_path LIKE '%/-/pipelines/charts' 
          THEN 'Analytics - CI/CD'
        WHEN page_url_path LIKE '%/-/analytics/code_reviews' 
          THEN 'Analytics - Code review'
        WHEN page_url_path LIKE '%/insights' 
          THEN 'Analytics - Insights'
        WHEN page_url_path LIKE '%/-/analytics/issues_analytics' 
          THEN 'Analytics - Issue'
        WHEN page_url_path LIKE '%/-/analytics/merge_request_analytics' 
          THEN 'Analytics - Merge request'
        WHEN page_url_path LIKE '%/-/graphs/main/charts' 
          THEN 'Analytics - Repository'
        WHEN page_url_path LIKE '%/-/snippets' 
          THEN 'Snippets'
        WHEN page_url_path LIKE '%/-/wikis/home' 
          THEN 'Wikis - Home'
        WHEN page_url_path LIKE '%/-/edit' 
          THEN 'Settings - Edit'
        WHEN page_url_path LIKE '%/-/settings/integrations' 
          THEN 'Settings - Integrations'
        WHEN page_url_path LIKE '%/-/projects' 
          THEN 'Settings - Projects'
        WHEN page_url_path LIKE '%/-/settings/repository' 
          THEN 'Settings - Repository'
        WHEN page_url_path LIKE '%/-/settings/ci_cd' 
          THEN 'Settings - CI/CD'
        WHEN page_url_path LIKE '%/-/settings/applications' 
          THEN 'Settings - Applications'
        WHEN page_url_path LIKE '%/-/settings/packages_and_registries' 
          THEN 'Settings - Packages and Registries'
        WHEN page_url_path LIKE '%/-/hooks' 
          THEN 'Settings - Hooks'
        WHEN page_url_path LIKE '%/-/usage_quotas#seats-quota-tab' 
          THEN 'Settings - Usage Quotas'
        WHEN page_url_path LIKE '%/-/billings' 
          THEN 'Settings - Billings'  
        ELSE 'Other' 
      END AS url_path_category,
      CASE 
       WHEN 
       ( 
        page_url_path like '%/security/dashboard%'
          or page_url_path like '%/security/dasboard%'
          or page_url_path like '%/security/vulnerabilities%'
          or page_url_path like '%/security/vulnerability_report%'
          or page_url_path like '/-/security%'
          or page_url_path like '%/pipelines/%/securit%' -- pipeline report
       )
       and page_url_path not like '%/commit/%' -- ignore commits to branches named 'security'
       and page_url_path not like '%/tree/%' -- ignore branches named 'security'
         THEN 1
       ELSE 0
      END AS is_url_interacting_with_security

    FROM page

)

{{ dbt_audit(
    cte_ref="dim_with_sk",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-07-22",
    updated_date="2022-11-02"
) }}
