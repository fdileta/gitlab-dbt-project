{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('subscriptions', 'bdg_subscription_product_rate_plan'),
    ('dates', 'dim_date'),
    ('seat_link', 'fct_usage_self_managed_seat_link'),
    ('ping_instance_wave', 'fct_ping_instance_metric_wave')
]) }}


, sm_subscriptions AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      first_day_of_month                                            AS snapshot_month
    FROM subscriptions
    INNER JOIN dates
      ON dates.date_actual BETWEEN '2017-04-01' AND CURRENT_DATE    -- first month Usage Ping was collected
    WHERE product_delivery_type = 'Self-Managed'
    {{ dbt_utils.group_by(n=4)}}


), ping_instance_wave_sm AS (

    SELECT *
    FROM ping_instance_wave
    WHERE dim_subscription_id IS NOT NULL
      AND ping_delivery_type = 'Self-Managed'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        dim_instance_id,
        hostname,
        ping_created_date_month
      ORDER BY ping_created_at DESC
      ) = 1

), joined AS (

    SELECT
      sm_subscriptions.dim_subscription_id,
      sm_subscriptions.dim_subscription_id_original,
      sm_subscriptions.dim_billing_account_id,
      sm_subscriptions.snapshot_month,
      {{ get_date_id('sm_subscriptions.snapshot_month') }}                                               AS snapshot_date_id,
      seat_link.report_date                                                                              AS seat_link_report_date,
      {{ get_date_id('seat_link.report_date') }}                                                         AS seat_link_report_date_id,
      ping_instance_wave_sm.dim_ping_instance_id,
      ping_instance_wave_sm.ping_created_at,
      {{ get_date_id('ping_instance_wave_sm.ping_created_at') }}                                         AS ping_created_date_id,
      ping_instance_wave_sm.dim_instance_id,
      ping_instance_wave_sm.hostname,
      --ping_instance_wave_sm.instance_type,
      ping_instance_wave_sm.dim_license_id,
      ping_instance_wave_sm.license_md5,
      --ping_instance_wave_sm.cleaned_version,
      ping_instance_wave_sm.dim_location_country_id,
      -- Wave 1
      DIV0(
          ping_instance_wave_sm.license_billable_users,
          IFNULL(ping_instance_wave_sm.license_user_count, seat_link.license_user_count)
          )                                                                                              AS license_utilization,
      ping_instance_wave_sm.license_billable_users                                                       AS billable_user_count,
      ping_instance_wave_sm.instance_user_count                                                          AS active_user_count,
      IFNULL(ping_instance_wave_sm.historical_max_user_count, seat_link.max_historical_user_count)       AS max_historical_user_count,
      IFNULL(ping_instance_wave_sm.license_user_count, seat_link.license_user_count)                     AS license_user_count,
      -- Wave 2 & 3
      ping_instance_wave_sm.umau_28_days_user,
      ping_instance_wave_sm.action_monthly_active_users_project_repo_28_days_user,
      ping_instance_wave_sm.merge_requests_28_days_user,
      ping_instance_wave_sm.projects_with_repositories_enabled_28_days_user,
      ping_instance_wave_sm.commit_comment_all_time_event,
      ping_instance_wave_sm.source_code_pushes_all_time_event,
      ping_instance_wave_sm.ci_pipelines_28_days_user,
      ping_instance_wave_sm.ci_internal_pipelines_28_days_user,
      ping_instance_wave_sm.ci_builds_28_days_user,
      ping_instance_wave_sm.ci_builds_all_time_user,
      ping_instance_wave_sm.ci_builds_all_time_event,
      ping_instance_wave_sm.ci_runners_all_time_event,
      ping_instance_wave_sm.auto_devops_enabled_all_time_event,
      ping_instance_wave_sm.gitlab_shared_runners_enabled,
      ping_instance_wave_sm.container_registry_enabled,
      ping_instance_wave_sm.template_repositories_all_time_event,
      ping_instance_wave_sm.ci_pipeline_config_repository_28_days_user,
      ping_instance_wave_sm.user_unique_users_all_secure_scanners_28_days_user,
      ping_instance_wave_sm.user_sast_jobs_28_days_user,
      ping_instance_wave_sm.user_dast_jobs_28_days_user,
      ping_instance_wave_sm.user_dependency_scanning_jobs_28_days_user,
      ping_instance_wave_sm.user_license_management_jobs_28_days_user,
      ping_instance_wave_sm.user_secret_detection_jobs_28_days_user,
      ping_instance_wave_sm.user_container_scanning_jobs_28_days_user,
      ping_instance_wave_sm.object_store_packages_enabled,
      ping_instance_wave_sm.projects_with_packages_all_time_event,
      ping_instance_wave_sm.projects_with_packages_28_days_user,
      ping_instance_wave_sm.deployments_28_days_user,
      ping_instance_wave_sm.releases_28_days_user,
      ping_instance_wave_sm.epics_28_days_user,
      ping_instance_wave_sm.issues_28_days_user,
      -- Wave 3.1
      ping_instance_wave_sm.ci_internal_pipelines_all_time_event,
      ping_instance_wave_sm.ci_external_pipelines_all_time_event,
      ping_instance_wave_sm.merge_requests_all_time_event,
      ping_instance_wave_sm.todos_all_time_event,
      ping_instance_wave_sm.epics_all_time_event,
      ping_instance_wave_sm.issues_all_time_event,
      ping_instance_wave_sm.projects_all_time_event,
      ping_instance_wave_sm.deployments_28_days_event,
      ping_instance_wave_sm.packages_28_days_event,
      ping_instance_wave_sm.sast_jobs_all_time_event,
      ping_instance_wave_sm.dast_jobs_all_time_event,
      ping_instance_wave_sm.dependency_scanning_jobs_all_time_event,
      ping_instance_wave_sm.license_management_jobs_all_time_event,
      ping_instance_wave_sm.secret_detection_jobs_all_time_event,
      ping_instance_wave_sm.container_scanning_jobs_all_time_event,
      ping_instance_wave_sm.projects_jenkins_active_all_time_event,
      ping_instance_wave_sm.projects_bamboo_active_all_time_event,
      ping_instance_wave_sm.projects_jira_active_all_time_event,
      ping_instance_wave_sm.projects_drone_ci_active_all_time_event,
      ping_instance_wave_sm.projects_github_active_all_time_event,
      ping_instance_wave_sm.projects_jira_server_active_all_time_event,
      ping_instance_wave_sm.projects_jira_dvcs_cloud_active_all_time_event,
      ping_instance_wave_sm.projects_with_repositories_enabled_all_time_event,
      ping_instance_wave_sm.protected_branches_all_time_event,
      ping_instance_wave_sm.remote_mirrors_all_time_event,
      ping_instance_wave_sm.projects_enforcing_code_owner_approval_28_days_user,
      ping_instance_wave_sm.project_clusters_enabled_28_days_user,
      ping_instance_wave_sm.analytics_28_days_user,                           
      ping_instance_wave_sm.issues_edit_28_days_user,                         
      ping_instance_wave_sm.user_packages_28_days_user,                            
      ping_instance_wave_sm.terraform_state_api_28_days_user,                          
      ping_instance_wave_sm.incident_management_28_days_user,                            
      -- Wave 3.2
      ping_instance_wave_sm.auto_devops_enabled,
      ping_instance_wave_sm.gitaly_clusters_instance,
      ping_instance_wave_sm.epics_deepest_relationship_level_instance,
      ping_instance_wave_sm.clusters_applications_cilium_all_time_event,
      ping_instance_wave_sm.network_policy_forwards_all_time_event,
      ping_instance_wave_sm.network_policy_drops_all_time_event,
      ping_instance_wave_sm.requirements_with_test_report_all_time_event,
      ping_instance_wave_sm.requirement_test_reports_ci_all_time_event,
      ping_instance_wave_sm.projects_imported_from_github_all_time_event,
      ping_instance_wave_sm.projects_jira_cloud_active_all_time_event,
      ping_instance_wave_sm.projects_jira_dvcs_server_active_all_time_event,
      ping_instance_wave_sm.service_desk_issues_all_time_event,
      ping_instance_wave_sm.ci_pipelines_all_time_user,
      ping_instance_wave_sm.service_desk_issues_28_days_user,
      ping_instance_wave_sm.projects_jira_active_28_days_user,
      ping_instance_wave_sm.projects_jira_dvcs_cloud_active_28_days_user,
      ping_instance_wave_sm.projects_jira_dvcs_server_active_28_days_user,
      ping_instance_wave_sm.merge_requests_with_required_code_owners_28_days_user,
      ping_instance_wave_sm.analytics_value_stream_28_days_event,
      ping_instance_wave_sm.code_review_user_approve_mr_28_days_user,
      ping_instance_wave_sm.epics_usage_28_days_user,
      ping_instance_wave_sm.ci_templates_usage_28_days_event,
      ping_instance_wave_sm.project_management_issue_milestone_changed_28_days_user,
      ping_instance_wave_sm.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      ping_instance_wave_sm.protected_branches_28_days_user,
      ping_instance_wave_sm.ci_cd_lead_time_usage_28_days_event,
      ping_instance_wave_sm.ci_cd_deployment_frequency_usage_28_days_event,
      ping_instance_wave_sm.projects_with_repositories_enabled_all_time_user,
      ping_instance_wave_sm.api_fuzzing_jobs_usage_28_days_user,
      ping_instance_wave_sm.coverage_fuzzing_pipeline_usage_28_days_event,
      ping_instance_wave_sm.api_fuzzing_pipeline_usage_28_days_event,
      ping_instance_wave_sm.container_scanning_pipeline_usage_28_days_event,
      ping_instance_wave_sm.dependency_scanning_pipeline_usage_28_days_event,
      ping_instance_wave_sm.sast_pipeline_usage_28_days_event,
      ping_instance_wave_sm.secret_detection_pipeline_usage_28_days_event,
      ping_instance_wave_sm.dast_pipeline_usage_28_days_event,
      ping_instance_wave_sm.coverage_fuzzing_jobs_28_days_user,
      ping_instance_wave_sm.environments_all_time_event,
      ping_instance_wave_sm.feature_flags_all_time_event,
      ping_instance_wave_sm.successful_deployments_28_days_event,
      ping_instance_wave_sm.failed_deployments_28_days_event,
      ping_instance_wave_sm.projects_compliance_framework_all_time_event,
      ping_instance_wave_sm.commit_ci_config_file_28_days_user,
      ping_instance_wave_sm.view_audit_all_time_user,
      --Wave 5.2
      ping_instance_wave_sm.dependency_scanning_jobs_all_time_user,
      ping_instance_wave_sm.analytics_devops_adoption_all_time_user,
      ping_instance_wave_sm.projects_imported_all_time_event,
      ping_instance_wave_sm.preferences_security_dashboard_28_days_user,
      ping_instance_wave_sm.web_ide_edit_28_days_user,
      ping_instance_wave_sm.auto_devops_pipelines_all_time_event,
      ping_instance_wave_sm.projects_prometheus_active_all_time_event,
      ping_instance_wave_sm.prometheus_enabled,
      ping_instance_wave_sm.prometheus_metrics_enabled,
      ping_instance_wave_sm.group_saml_enabled,
      ping_instance_wave_sm.jira_issue_imports_all_time_event,
      ping_instance_wave_sm.author_epic_all_time_user,
      ping_instance_wave_sm.author_issue_all_time_user,
      ping_instance_wave_sm.failed_deployments_28_days_user,
      ping_instance_wave_sm.successful_deployments_28_days_user,
      -- Wave 5.3
      ping_instance_wave_sm.geo_enabled,
      ping_instance_wave_sm.geo_nodes_all_time_event,
      ping_instance_wave_sm.auto_devops_pipelines_28_days_user,
      ping_instance_wave_sm.active_instance_runners_all_time_event,
      ping_instance_wave_sm.active_group_runners_all_time_event,
      ping_instance_wave_sm.active_project_runners_all_time_event,
      ping_instance_wave_sm.gitaly_version,
      ping_instance_wave_sm.gitaly_servers_all_time_event,
      -- Wave 6
      ping_instance_wave_sm.api_fuzzing_scans_all_time_event,
      ping_instance_wave_sm.api_fuzzing_scans_28_days_event,
      ping_instance_wave_sm.coverage_fuzzing_scans_all_time_event,
      ping_instance_wave_sm.coverage_fuzzing_scans_28_days_event,
      ping_instance_wave_sm.secret_detection_scans_all_time_event,
      ping_instance_wave_sm.secret_detection_scans_28_days_event,
      ping_instance_wave_sm.dependency_scanning_scans_all_time_event,
      ping_instance_wave_sm.dependency_scanning_scans_28_days_event,
      ping_instance_wave_sm.container_scanning_scans_all_time_event,
      ping_instance_wave_sm.container_scanning_scans_28_days_event,
      ping_instance_wave_sm.dast_scans_all_time_event,
      ping_instance_wave_sm.dast_scans_28_days_event,
      ping_instance_wave_sm.sast_scans_all_time_event,
      ping_instance_wave_sm.sast_scans_28_days_event,
      -- Data Quality Flags
      IFF(ping_instance_wave_sm.instance_user_count != seat_link.active_user_count,
          ping_instance_wave_sm.instance_user_count, NULL)                                               AS instance_user_count_not_aligned,
      IFF(ping_instance_wave_sm.historical_max_user_count != seat_link.max_historical_user_count,
          ping_instance_wave_sm.historical_max_user_count, NULL)                                         AS historical_max_users_not_aligned,
      seat_link.is_subscription_in_zuora                                                                 AS is_seat_link_subscription_in_zuora,
      seat_link.is_rate_plan_in_zuora                                                                    AS is_seat_link_rate_plan_in_zuora,
      seat_link.is_active_user_count_available                                                           AS is_seat_link_active_user_count_available,
      ping_instance_wave_sm.is_license_mapped_to_subscription                                            AS is_usage_ping_license_mapped_to_subscription,
      ping_instance_wave_sm.is_license_subscription_id_valid                                             AS is_usage_ping_license_subscription_id_valid,
      IFF(ping_instance_wave_sm.ping_created_at IS NOT NULL
            OR seat_link.report_date IS NOT NULL,
          TRUE, FALSE)                                                                                   AS is_data_in_subscription_month,
      IFF(is_data_in_subscription_month = TRUE AND
            ROW_NUMBER() OVER (PARTITION BY
                                sm_subscriptions.dim_subscription_id,
                                ping_instance_wave_sm.dim_instance_id,
                                ping_instance_wave_sm.hostname,
                                is_data_in_subscription_month
                               ORDER BY sm_subscriptions.snapshot_month DESC
                            ) = 1,
          TRUE, FALSE)                                                                                   AS is_latest_data
    FROM sm_subscriptions
    LEFT JOIN ping_instance_wave_sm
      ON sm_subscriptions.dim_subscription_id = ping_instance_wave_sm.dim_subscription_id
      AND sm_subscriptions.snapshot_month = ping_instance_wave_sm.ping_created_date_month
    LEFT JOIN seat_link
      ON sm_subscriptions.dim_subscription_id = seat_link.dim_subscription_id
      AND sm_subscriptions.snapshot_month = seat_link.snapshot_month

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-21",
    updated_date="2022-07-21"
) }}
