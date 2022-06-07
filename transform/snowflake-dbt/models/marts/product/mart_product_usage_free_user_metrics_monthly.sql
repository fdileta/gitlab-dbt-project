{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_product"
  })
}}


{{ simple_cte([
    ('free_user_metrics', 'fct_product_usage_free_user_metrics_monthly'),
    ('crm_accounts', 'dim_crm_account'),
    ('namespaces', 'dim_namespace')
]) }}

, joined AS (

    SELECT
      free_user_metrics.reporting_month,
      free_user_metrics.dim_namespace_id,
      namespaces.namespace_name,
      free_user_metrics.uuid,
      free_user_metrics.hostname,
      free_user_metrics.delivery_type,
      free_user_metrics.cleaned_version,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      crm_account_name,
      parent_crm_account_name,
      free_user_metrics.ping_date,
      -- Wave 2 & 3
      free_user_metrics.umau_28_days_user,
      free_user_metrics.action_monthly_active_users_project_repo_28_days_user,
      free_user_metrics.merge_requests_28_days_user,
      free_user_metrics.projects_with_repositories_enabled_28_days_user,
      free_user_metrics.commit_comment_all_time_event,
      free_user_metrics.source_code_pushes_all_time_event,
      free_user_metrics.ci_pipelines_28_days_user,
      free_user_metrics.ci_internal_pipelines_28_days_user,
      free_user_metrics.ci_builds_28_days_user,
      free_user_metrics.ci_builds_all_time_user,
      free_user_metrics.ci_builds_all_time_event,
      free_user_metrics.ci_runners_all_time_event,
      free_user_metrics.auto_devops_enabled_all_time_event,
      free_user_metrics.gitlab_shared_runners_enabled,
      free_user_metrics.container_registry_enabled,
      free_user_metrics.template_repositories_all_time_event,
      free_user_metrics.ci_pipeline_config_repository_28_days_user,
      free_user_metrics.user_unique_users_all_secure_scanners_28_days_user,
      free_user_metrics.user_sast_jobs_28_days_user,
      free_user_metrics.user_dast_jobs_28_days_user,
      free_user_metrics.user_dependency_scanning_jobs_28_days_user,
      free_user_metrics.user_license_management_jobs_28_days_user,
      free_user_metrics.user_secret_detection_jobs_28_days_user,
      free_user_metrics.user_container_scanning_jobs_28_days_user,
      free_user_metrics.object_store_packages_enabled,
      free_user_metrics.projects_with_packages_all_time_event,
      free_user_metrics.projects_with_packages_28_days_user,
      free_user_metrics.deployments_28_days_user,
      free_user_metrics.releases_28_days_user,
      free_user_metrics.epics_28_days_user,
      free_user_metrics.issues_28_days_user,
      -- Wave 3.1
      free_user_metrics.ci_internal_pipelines_all_time_event,
      free_user_metrics.ci_external_pipelines_all_time_event,
      free_user_metrics.merge_requests_all_time_event,
      free_user_metrics.todos_all_time_event,
      free_user_metrics.epics_all_time_event,
      free_user_metrics.issues_all_time_event,
      free_user_metrics.projects_all_time_event,
      free_user_metrics.deployments_28_days_event,
      free_user_metrics.packages_28_days_event,
      free_user_metrics.sast_jobs_all_time_event,
      free_user_metrics.dast_jobs_all_time_event,
      free_user_metrics.dependency_scanning_jobs_all_time_event,
      free_user_metrics.license_management_jobs_all_time_event,
      free_user_metrics.secret_detection_jobs_all_time_event,
      free_user_metrics.container_scanning_jobs_all_time_event,
      free_user_metrics.projects_jenkins_active_all_time_event,
      free_user_metrics.projects_bamboo_active_all_time_event,
      free_user_metrics.projects_jira_active_all_time_event,
      free_user_metrics.projects_drone_ci_active_all_time_event,
      free_user_metrics.projects_github_active_all_time_event,
      free_user_metrics.projects_jira_server_active_all_time_event,
      free_user_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      free_user_metrics.projects_with_repositories_enabled_all_time_event,
      free_user_metrics.protected_branches_all_time_event,
      free_user_metrics.remote_mirrors_all_time_event,
      free_user_metrics.projects_enforcing_code_owner_approval_28_days_user,
      free_user_metrics.project_clusters_enabled_28_days_user,
      free_user_metrics.analytics_28_days_user,
      free_user_metrics.issues_edit_28_days_user,
      free_user_metrics.user_packages_28_days_user,
      free_user_metrics.terraform_state_api_28_days_user,
      free_user_metrics.incident_management_28_days_user,
      -- Wave 3.2
      free_user_metrics.auto_devops_enabled,
      free_user_metrics.gitaly_clusters_instance,
      free_user_metrics.epics_deepest_relationship_level_instance,
      free_user_metrics.clusters_applications_cilium_all_time_event,
      free_user_metrics.network_policy_forwards_all_time_event,
      free_user_metrics.network_policy_drops_all_time_event,
      free_user_metrics.requirements_with_test_report_all_time_event,
      free_user_metrics.requirement_test_reports_ci_all_time_event,
      free_user_metrics.projects_imported_from_github_all_time_event,
      free_user_metrics.projects_jira_cloud_active_all_time_event,
      free_user_metrics.projects_jira_dvcs_server_active_all_time_event,
      free_user_metrics.service_desk_issues_all_time_event,
      free_user_metrics.ci_pipelines_all_time_user,
      free_user_metrics.service_desk_issues_28_days_user,
      free_user_metrics.projects_jira_active_28_days_user,
      free_user_metrics.projects_jira_dvcs_cloud_active_28_days_user,
      free_user_metrics.projects_jira_dvcs_server_active_28_days_user,
      free_user_metrics.merge_requests_with_required_code_owners_28_days_user,
      free_user_metrics.analytics_value_stream_28_days_event,
      free_user_metrics.code_review_user_approve_mr_28_days_user,
      free_user_metrics.epics_usage_28_days_user,
      free_user_metrics.ci_templates_usage_28_days_event,
      free_user_metrics.project_management_issue_milestone_changed_28_days_user,
      free_user_metrics.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      free_user_metrics.protected_branches_28_days_user,
      free_user_metrics.ci_cd_lead_time_usage_28_days_event,
      free_user_metrics.ci_cd_deployment_frequency_usage_28_days_event,
      free_user_metrics.projects_with_repositories_enabled_all_time_user,
      free_user_metrics.api_fuzzing_jobs_usage_28_days_user,
      free_user_metrics.coverage_fuzzing_pipeline_usage_28_days_event,
      free_user_metrics.api_fuzzing_pipeline_usage_28_days_event,
      free_user_metrics.container_scanning_pipeline_usage_28_days_event,
      free_user_metrics.dependency_scanning_pipeline_usage_28_days_event,
      free_user_metrics.sast_pipeline_usage_28_days_event,
      free_user_metrics.secret_detection_pipeline_usage_28_days_event,
      free_user_metrics.dast_pipeline_usage_28_days_event,
      free_user_metrics.coverage_fuzzing_jobs_28_days_user,
      free_user_metrics.environments_all_time_event,
      free_user_metrics.feature_flags_all_time_event,
      free_user_metrics.successful_deployments_28_days_event,
      free_user_metrics.failed_deployments_28_days_event,
      free_user_metrics.projects_compliance_framework_all_time_event,
      free_user_metrics.commit_ci_config_file_28_days_user,
      free_user_metrics.view_audit_all_time_user,
      -- Wave 5.2
      free_user_metrics.dependency_scanning_jobs_all_time_user,
      free_user_metrics.analytics_devops_adoption_all_time_user,
      free_user_metrics.projects_imported_all_time_event,
      free_user_metrics.preferences_security_dashboard_28_days_user,
      free_user_metrics.web_ide_edit_28_days_user,
      free_user_metrics.auto_devops_pipelines_all_time_event,
      free_user_metrics.projects_prometheus_active_all_time_event,
      free_user_metrics.prometheus_enabled,
      free_user_metrics.prometheus_metrics_enabled,
      free_user_metrics.group_saml_enabled,
      free_user_metrics.jira_issue_imports_all_time_event,
      free_user_metrics.author_epic_all_time_user,
      free_user_metrics.author_issue_all_time_user,
      free_user_metrics.failed_deployments_28_days_user,
      free_user_metrics.successful_deployments_28_days_user,
      -- Wave 5.3
      free_user_metrics.geo_enabled,
      free_user_metrics.geo_nodes_all_time_event,
      free_user_metrics.auto_devops_pipelines_28_days_user,
      free_user_metrics.active_instance_runners_all_time_event,
      free_user_metrics.active_group_runners_all_time_event,
      free_user_metrics.active_project_runners_all_time_event,
      free_user_metrics.gitaly_version,
      free_user_metrics.gitaly_servers_all_time_event,
      -- Wave 6
      free_user_metrics.api_fuzzing_scans_all_time_event,
      free_user_metrics.api_fuzzing_scans_28_days_event,
      free_user_metrics.coverage_fuzzing_scans_all_time_event,
      free_user_metrics.coverage_fuzzing_scans_28_days_event,
      free_user_metrics.secret_detection_scans_all_time_event,
      free_user_metrics.secret_detection_scans_28_days_event,
      free_user_metrics.dependency_scanning_scans_all_time_event,
      free_user_metrics.dependency_scanning_scans_28_days_event,
      free_user_metrics.container_scanning_scans_all_time_event,
      free_user_metrics.container_scanning_scans_28_days_event,
      free_user_metrics.dast_scans_all_time_event,
      free_user_metrics.dast_scans_28_days_event,
      free_user_metrics.sast_scans_all_time_event,
      free_user_metrics.sast_scans_28_days_event,   
      -- Data Quality Flag
      free_user_metrics.is_latest_data
    FROM free_user_metrics
    LEFT JOIN crm_accounts
      ON free_user_metrics.dim_crm_account_id = crm_accounts.dim_crm_account_id
    LEFT JOIN namespaces
      ON namespaces.dim_namespace_id = free_user_metrics.dim_namespace_id

)

{{ hash_diff(
    cte_ref="joined",
    return_cte="final",
    columns=[
        'reporting_month',
        'dim_namespace_id',
        'uuid',
        'hostname',
        'delivery_type',
        'cleaned_version',
        'dim_crm_account_id',
        'crm_account_name',
        'parent_crm_account_name',
        'ping_date',
        'umau_28_days_user',
        'action_monthly_active_users_project_repo_28_days_user',
        'merge_requests_28_days_user',
        'projects_with_repositories_enabled_28_days_user',
        'commit_comment_all_time_event',
        'source_code_pushes_all_time_event',
        'ci_pipelines_28_days_user',
        'ci_internal_pipelines_28_days_user',
        'ci_builds_28_days_user',
        'ci_builds_all_time_user',
        'ci_builds_all_time_event',
        'ci_runners_all_time_event',
        'auto_devops_enabled_all_time_event',
        'gitlab_shared_runners_enabled',
        'container_registry_enabled',
        'template_repositories_all_time_event',
        'ci_pipeline_config_repository_28_days_user',
        'user_unique_users_all_secure_scanners_28_days_user',
        'user_sast_jobs_28_days_user',
        'user_dast_jobs_28_days_user',
        'user_dependency_scanning_jobs_28_days_user',
        'user_license_management_jobs_28_days_user',
        'user_secret_detection_jobs_28_days_user',
        'user_container_scanning_jobs_28_days_user',
        'object_store_packages_enabled',
        'projects_with_packages_all_time_event',
        'projects_with_packages_28_days_user',
        'deployments_28_days_user',
        'releases_28_days_user',
        'epics_28_days_user',
        'issues_28_days_user',
        'ci_internal_pipelines_all_time_event',
        'ci_external_pipelines_all_time_event',
        'merge_requests_all_time_event',
        'todos_all_time_event',
        'epics_all_time_event',
        'issues_all_time_event',
        'projects_all_time_event',
        'deployments_28_days_event',
        'packages_28_days_event',
        'sast_jobs_all_time_event',
        'dast_jobs_all_time_event',
        'dependency_scanning_jobs_all_time_event',
        'license_management_jobs_all_time_event',
        'secret_detection_jobs_all_time_event',
        'container_scanning_jobs_all_time_event',
        'projects_jenkins_active_all_time_event',
        'projects_bamboo_active_all_time_event',
        'projects_jira_active_all_time_event',
        'projects_drone_ci_active_all_time_event',
        'projects_github_active_all_time_event',
        'projects_jira_server_active_all_time_event',
        'projects_jira_dvcs_cloud_active_all_time_event',
        'projects_with_repositories_enabled_all_time_event',
        'protected_branches_all_time_event',
        'remote_mirrors_all_time_event',
        'projects_enforcing_code_owner_approval_28_days_user',
        'project_clusters_enabled_28_days_user',
        'analytics_28_days_user',
        'issues_edit_28_days_user',
        'user_packages_28_days_user',
        'terraform_state_api_28_days_user',
        'incident_management_28_days_user',
        'auto_devops_enabled',
        'gitaly_clusters_instance',
        'epics_deepest_relationship_level_instance',
        'clusters_applications_cilium_all_time_event',
        'network_policy_forwards_all_time_event',
        'network_policy_drops_all_time_event',
        'requirements_with_test_report_all_time_event',
        'requirement_test_reports_ci_all_time_event',
        'projects_imported_from_github_all_time_event',
        'projects_jira_cloud_active_all_time_event',
        'projects_jira_dvcs_server_active_all_time_event',
        'service_desk_issues_all_time_event',
        'ci_pipelines_all_time_user',
        'service_desk_issues_28_days_user',
        'projects_jira_active_28_days_user',
        'projects_jira_dvcs_cloud_active_28_days_user',
        'projects_jira_dvcs_server_active_28_days_user',
        'merge_requests_with_required_code_owners_28_days_user',
        'analytics_value_stream_28_days_event',
        'code_review_user_approve_mr_28_days_user',
        'epics_usage_28_days_user',
        'ci_templates_usage_28_days_event',
        'project_management_issue_milestone_changed_28_days_user',
        'project_management_issue_iteration_changed_28_days_user',
        'protected_branches_28_days_user',
        'ci_cd_lead_time_usage_28_days_event',
        'ci_cd_deployment_frequency_usage_28_days_event',
        'projects_with_repositories_enabled_all_time_user',
        'api_fuzzing_jobs_usage_28_days_user',
        'coverage_fuzzing_pipeline_usage_28_days_event',
        'api_fuzzing_pipeline_usage_28_days_event',
        'container_scanning_pipeline_usage_28_days_event',
        'dependency_scanning_pipeline_usage_28_days_event',
        'sast_pipeline_usage_28_days_event',
        'secret_detection_pipeline_usage_28_days_event',
        'dast_pipeline_usage_28_days_event',
        'coverage_fuzzing_jobs_28_days_user',
        'environments_all_time_event',
        'feature_flags_all_time_event',
        'successful_deployments_28_days_event',
        'failed_deployments_28_days_event',
        'projects_compliance_framework_all_time_event',
        'commit_ci_config_file_28_days_user',
        'view_audit_all_time_user',
        'dependency_scanning_jobs_all_time_user',
        'analytics_devops_adoption_all_time_user',
        'projects_imported_all_time_event',
        'preferences_security_dashboard_28_days_user',
        'web_ide_edit_28_days_user',
        'auto_devops_pipelines_all_time_event',
        'projects_prometheus_active_all_time_event',
        'prometheus_enabled',
        'prometheus_metrics_enabled',
        'group_saml_enabled',
        'jira_issue_imports_all_time_event',
        'author_epic_all_time_user',
        'author_issue_all_time_user',
        'failed_deployments_28_days_user',
        'successful_deployments_28_days_user',
        'geo_enabled',
        'geo_nodes_all_time_event',
        'auto_devops_pipelines_28_days_user',
        'active_instance_runners_all_time_event',
        'active_group_runners_all_time_event',
        'active_project_runners_all_time_event',
        'gitaly_version',
        'gitaly_servers_all_time_event',
        'api_fuzzing_scans_all_time_event',
        'api_fuzzing_scans_28_days_event',
        'coverage_fuzzing_scans_all_time_event',
        'coverage_fuzzing_scans_28_days_event',
        'secret_detection_scans_all_time_event',
        'secret_detection_scans_28_days_event',
        'dependency_scanning_scans_all_time_event',
        'dependency_scanning_scans_28_days_event',
        'container_scanning_scans_all_time_event',
        'container_scanning_scans_28_days_event',
        'dast_scans_all_time_event',
        'dast_scans_28_days_event',
        'sast_scans_all_time_event',
        'sast_scans_28_days_event',
        'is_latest_data'
    ]
) }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@mdrussell",
    created_date="2021-06-14",
    updated_date="2022-06-01"
) }}
