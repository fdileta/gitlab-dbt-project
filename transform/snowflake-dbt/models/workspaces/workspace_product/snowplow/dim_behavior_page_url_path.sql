WITH page_url_information AS (

  SELECT DISTINCT

    --surrogate key
    {{ dbt_utils.surrogate_key(['prep_snowplow_structured_event_all_source.page_url_path']) }} AS dim_behavior_page_url_path_sk,

    --natural keys
    prep_snowplow_structured_event_all_source.page_url_path,

    -- 
    {{ clean_url('prep_snowplow_structured_event_all_source.page_url_path') }}  AS clean_url_path,

    -- attributes
    REGEXP_SUBSTR(prep_snowplow_structured_event_all_source.page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1) AS url_namespace_id,
    REGEXP_SUBSTR(prep_snowplow_structured_event_all_source.page_url_path, 'project(\\d+)', 1, 1, 'e', 1) AS url_project_id,
    CASE 
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/activity' 
        THEN 'Project information - Activity'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/labels' 
        THEN 'Project information - Labels'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/project_members' 
        THEN 'Project information - Members'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/tree/main' 
        THEN 'Repository - Files'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/commits/main' 
        THEN 'Repository - Commits'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/branches' 
        THEN 'Repository - Branches'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/tags' 
        THEN 'Repository - Tags'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/graphs/main' 
        THEN 'Repository - Contributors'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/network/main' 
        THEN 'Repository - Graph'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/compare' 
        THEN 'Repository - Compare'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/path_locks' 
        THEN 'Repository - Locked Files'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/issues/service_desk' 
        THEN 'Issues - Service Desk'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/issues' 
        THEN 'Issues - List'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/boards' 
        THEN 'Issues - Boards'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/milestones' 
        THEN 'Issues - Milestones'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/iterations' 
        THEN 'Issues - Iterations'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/requirements_management/requirements' 
        THEN 'Issues - Requirements'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/merge_requests' 
        THEN 'Merge requests'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/pipelines' 
        THEN 'CI/CD - Pipelines'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/ci/editor' 
        THEN 'CI/CD - Editor'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/jobs' 
        THEN 'CI/CD - Jobs'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/pipeline_schedules' 
        THEN 'CI/CD - Schedules'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/quality/test_cases' 
        THEN 'CI/CD - Test Cases'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/security/dashboard' 
        THEN 'Security & Compliance - Security dashboard'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/security/vulnerability_report' 
        THEN 'Security & Compliance - Vulnerability report'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/on_demand_scans' 
        THEN 'Security & Compliance - On-demand scans'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/dependencies' 
        THEN 'Security & Compliance - Dependency list'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/licenses' 
        THEN 'Security & Compliance - License compliance'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/security/policies' 
        THEN 'Security & Compliance - Policies'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/audit_events' 
        THEN 'Security & Compliance - Audit events'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/security/configuration' 
        THEN 'Security & Compliance - Configuration'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/feature_flags' 
        THEN 'Deployments - Feature Flags'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/environments' 
        THEN 'Deployments - Environments'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/releases' 
        THEN 'Deployments - Releases'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/metrics' 
        THEN 'Monitor - Metrics'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/logs' 
        THEN 'Monitor - Logs'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/error_tracking' 
        THEN 'Monitor - Error Tracking'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/alert_management' 
        THEN 'Monitor - Alerts'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/incidents' 
        THEN 'Monitor - Incidents'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/oncall_schedules' 
        THEN 'Monitor - On-call Schedules'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/escalation_policies' 
        THEN 'Monitor - Escalation Policies'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/clusters' 
        THEN 'Infrastructure - Kubernetes clusters'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/serverless/functions' 
        THEN 'Infrastructure - Serverless platform'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/terraform' 
        THEN 'Infrastructure - Terraform'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/packages' 
        THEN 'Packages & Registries - Package Registry'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/container_registry' 
        THEN 'Packages & Registries - Container Registry'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/infastructure_registry' 
        THEN 'Packages & Registries - Infastructure Registry'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/value_stream_analytics' 
        THEN 'Analytics - Value stream'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/pipelines/charts' 
        THEN 'Analytics - CI/CD'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/analytics/code_reviews' 
        THEN 'Analytics - Code review'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/insights' 
        THEN 'Analytics - Insights'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/analytics/issues_analytics' 
        THEN 'Analytics - Issue'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/analytics/merge_request_analytics' 
        THEN 'Analytics - Merge request'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/graphs/main/charts' 
        THEN 'Analytics - Repository'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/snippets' 
        THEN 'Snippets'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/wikis/home' 
        THEN 'Wikis - Home'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/edit' 
        THEN 'Settings - Edit'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/settings/integrations' 
        THEN 'Settings - Integrations'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/projects' 
        THEN 'Settings - Projects'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/settings/repository' 
        THEN 'Settings - Repository'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/settings/ci_cd' 
        THEN 'Settings - CI/CD'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/settings/applications' 
        THEN 'Settings - Applications'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/settings/packages_and_registries' 
        THEN 'Settings - Packages and Registries'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/hooks' 
        THEN 'Settings - Hooks'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/usage_quotas#seats-quota-tab' 
        THEN 'Settings - Usage Quotas'
      WHEN prep_snowplow_structured_event_all_source.page_url_path LIKE '%/-/billings' 
        THEN 'Settings - Billings'  
      ELSE 'Other' 
    END AS url_path_category,
    CASE 
     WHEN 
     ( 
      prep_snowplow_structured_event_all_source.page_url_path like '%/security/dashboard%'
        or prep_snowplow_structured_event_all_source.page_url_path like '%/security/dasboard%'
        or prep_snowplow_structured_event_all_source.page_url_path like '%/security/vulnerabilities%'
        or prep_snowplow_structured_event_all_source.page_url_path like '%/security/vulnerability_report%'
        or prep_snowplow_structured_event_all_source.page_url_path like '/-/security%'
        or prep_snowplow_structured_event_all_source.page_url_path like '%/pipelines/%/securit%' -- pipeline report
     )
     and prep_snowplow_structured_event_all_source.page_url_path not like '%/commit/%' -- ignore commits to branches named 'security'
     and prep_snowplow_structured_event_all_source.page_url_path not like '%/tree/%' -- ignore branches named 'security'
       THEN 1
     ELSE 0
    END AS is_url_interacting_with_security


  FROM {{ ref('prep_snowplow_structured_event_all_source') }}

)

{{ dbt_audit(
    cte_ref="page_url_information",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-20",
    updated_date="2022-09-20"
) }}