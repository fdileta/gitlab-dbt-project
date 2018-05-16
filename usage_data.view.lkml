view: usage_data {
  sql_table_name: version.usage_data_unpacked ;;
  label: "Usage Data"

  dimension_group: timeframe {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.created_at ;;
  }

  dimension: hostname {
    label: "Host Name"
    type: string
    sql:${TABLE}.hostname ;;
  }

  dimension: version {
    label: "GitLab Version"
    type:  string
    full_suggestions: yes
    sql:  ${TABLE}.version ;;
  }

  dimension: mattermost_enabled {
    type: yesno
    sql:  ${TABLE}.mattermost_enabled ;;
  }

  dimension: has_license {
    type: yesno
    sql: ${TABLE}.license_md5 IS NOT NULL ;;
  }

  dimension: auto_devops_disabled {
    label: "Auto DevOps Disabled"
    type: number
    sql: ${TABLE}.auto_devops_disabled ;;
  }

  dimension: auto_devops_enabled {
    label: "Auto DevOps Enabled"
    type: number
    sql: ${TABLE}.auto_devops_enabled ;;
  }

  dimension: boards {
    type: number
    sql: ${TABLE}.boards ;;
  }

  dimension: protected_branches {
    type: number
    sql: ${TABLE}.protected_branches ;;
  }

  dimension: releases {
    type: number
    sql: ${TABLE}.releases ;;
  }

  dimension: remote_mirrors {
    type: number
    sql: ${TABLE}.remote_mirrors ;;
  }

  dimension: snippets {
    type: number
    sql: ${TABLE}.snippets ;;
  }

  dimension: todos {
    label: "TODOs"
    type: number
    sql: ${TABLE}.todos ;;
  }

  dimension: uploads {
    type: number
    sql: ${TABLE}.uploads ;;
  }

  dimension: web_hooks {
    type: number
    sql: ${TABLE}.web_hooks ;;
  }

  dimension: keys {
    type: number
    sql: ${TABLE}.keys ;;
  }

  dimension: labels {
    type: number
    sql: ${TABLE}.labels ;;
  }
  dimension: lfs_objects {
    label: "LFS Objects"
    type: number
    sql: ${TABLE}.lfs_objects ;;
  }

  dimension: milestones {

    type: number
    sql: ${TABLE}.milestones ;;
  }

  dimension: notes {
    type: number
    sql: ${TABLE}.{ ;;
  }

  dimension: pages_domains {
    type: number
    sql: ${TABLE}.pages_domains ;;
  }

  dimension: deploy_keys {
    type: number
    sql: ${TABLE}.deploy_keys ;;
  }

  dimension: environments {
    type: number
    sql: ${TABLE}.environments ;;
  }

  dimension: groups {
    type: number
    sql: ${TABLE}.groups ;;
  }

  dimension: in_review_folder {
    type: number
    sql: ${TABLE}.in_review_folder ;;
  }

  # UUID

  dimension: uuid {
    description: "Unique ID of GitLab Instance"
    label: "UUID"
    type: string
    sql: ${TABLE}.uuid ;;
  }

  measure: distinct_uuid_count {
    label: "Distinct UUID Count"
    type: count_distinct
    sql: ${uuid} ;;
  }

  # Active users

  dimension: active_user_count {
    label: "Active Users"
    description: "Returns NULL if 0 to avoid divide by zero errors."
    type: number
    sql: NULLIF(${TABLE}.active_user_count, 0) ;;
  }

  measure: average_users {
    group_label: "Averages"
    label: "Users per Instance"
    type: average
    sql: ${active_user_count} ;;
  }

  measure: percentile80_users {
    group_label: "80th Percentile Group"
    label: "Users per Instance"
    type: percentile
    percentile: 80
    sql: ${active_user_count} ;;
  }

  measure: percentile90_users {
    group_label: "90th Percentile Group"
    label: "Users per Instance"
    type: percentile
    percentile: 90
    sql: ${active_user_count} ;;
  }

  measure: percentile99_users {
    group_label: "99th Percentile Group"
    label: "Users per Instance"
    type: percentile
    percentile: 99
    sql: ${active_user_count} ;;
  }

  # Projects

  dimension: projects_count {
    label: "Projects"
    type: number
    sql: ${TABLE}.projects ;;
  }

  dimension: projects_imported_from_github {
    label: "Projects Imported from GitHub"
    type: number
    sql: ${TABLE}.projects_imported_from_github ;;
  }

  dimension: clusters_count {
    group_label: "Clusters"
    label: "Count"
    type: number
    sql: ${TABLE}.clusters ;;
  }

  measure: clusters {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_count} ;;
  }

  dimension: clusters_enabled {
    group_label: "Clusters"
    label: "Count Enabled"
    type: number
    sql: ${TABLE}.clusters_enabled ;;
  }

  measure: enabled_clusters {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_enabled} ;;
  }

  dimension: clusters_disabled {
    group_label: "Clusters"
    label: "Count Disabled"
    type: number
    sql:  ${TABLE}.clusters_disabled ;;
  }

  measure: disabled_clusters {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_disabled} ;;
  }

  dimension: clusters_platforms_gke {
    group_label: "Clusters"
    label: "Platforms GKE"
    type: number
    sql: ${TABLE}.clusters_platforms_gke ;;
  }

  measure: gke_clusters {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_platforms_gke} ;;
  }

  dimension: clusters_platforms_existing {
    group_label: "Clusters"
    label: "Platforms Existing"
    type: number
    sql: ${TABLE}.clusters_platforms_user ;;
  }

  measure: existing_clusters {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_platforms_existing} ;;
  }

  dimension: clusters_helm_deployed {
    group_label: "Clusters"
    label: "Helm Deployed"
    type: number
    sql: ${TABLE}clusters_applications_helm ;;
  }

  measure: helm_deployed {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_helm_deployed} ;;
  }

  dimension: clusters_ingress_deployed {
    group_label: "Clusters"
    label: "Ingress Deployed"
    type: number
    sql: ${TABLE}.clusters_applications_ingress ;;
  }

  measure: ingress_deployed {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_ingress_deployed} ;;
  }

  dimension: clusters_prometheus_deployed {
    group_label: "Clusters"
    label: "Prometheus Deployed"
    type: number
    sql: ${TABLE}.clusters_applications_prometheus ;;
  }

  measure: prometheus_deployed {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_prometheus_deployed} ;;
  }

  dimension: clusters_runner_deployed {
    group_label: "Clusters"
    label: "Runner Deployed"
    type: number
    sql: ${TABLE}.clusters_applications_runner ;;
  }

  measure: runner_deployed {
    group_label: "Clusters: Total"
    type: sum
    sql: ${clusters_runner_deployed} ;;
  }

  # Continuous Integration

  dimension: ci_builds {
    group_label: "CI Group"
    label: "Builds"
    type: number
    sql: ${TABLE}.ci_builds ;;
  }

  dimension: ci_deployments {
    group_label: "CI Group"
    label: "Deployments"
    type: number
    sql: ${TABLE}.deployments ;;
  }

  dimension: ci_internal_pipelines {
    group_label: "CI Group"
    label: "Internal Pipelines"
    type: number
    sql: ${TABLE}.ci_internal_pipelines ;;
  }

  dimension: ci_external_pipelines {
    group_label: "CI Group"
    label: "External Pipelines"
    type: number
    sql: ${TABLE}.ci_external_pipelines ;;
  }

  dimension: ci_pipeline_config_auto_devops {
    group_label: "CI Group"
    label: "Pipeline Config Auto DevOps"
    type: number
    sql: ${TABLE}.ci_pipeline_config_auto_devops ;;
  }

  dimension: ci_pipeline_config_repository {
    group_label: "CI Group"
    label: "Pipeline Config Repository"
    type: number
    sql: ${TABLE}.ci_pipeline_config_repository ;;
  }

  dimension: ci_pipeline_schedules {
    group_label: "CI Group"
    label: "Pipeline Schedules"
    type: number
    sql: ${TABLE}.ci_pipeline_schedules ;;
  }

  dimension: ci_runners {
    group_label: "CI Group"
    label: "Runners"
    type: number
    sql: ${TABLE}.ci_runners ;;
  }

  dimension: ci_triggers {
    group_label: "CI Group"
    label: "Triggers"
    type: number
    sql: ${TABLE}.ci_triggers ;;
  }

  # Projects

  measure: average_projects_per_user {
    group_label: "Averages"
    label: "Projects per User"
    type: average
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile80_projects_per_user {
    group_label: "80th Percentile Group"
    label: "Projects per User"
    type: percentile
    percentile: 80
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile90_projects_per_user {
    group_label: "90th Percentile Group"
    label: "Projects per User"
    type: percentile
    percentile: 90
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile99_projects_per_user {
    group_label: "99th Percentile Group"
    label: "Projects per User"
    type: percentile
    percentile: 99
    sql: ${projects_count} / ${active_user_count} ;;
  }

  # Issues

  dimension: issues_count {
    label: "Issues"
    type: number
    sql: ${TABLE}.issues ;;
  }

  measure: average_issues_per_user {

    group_label: "Averages"
    label: "Issues per User"
    type: average
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile80_issues_per_user {
    group_label: "80th Percentile Group"
    label: "Issues per User"
    type: percentile
    percentile: 80
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile90_issues_per_user {
    group_label: "90th Percentile Group"
    label: "Issues per User"
    type: percentile
    percentile: 90
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile99_issues_per_user {
    group_label: "99th Percentile Group"
    label: "Issues per User"
    type: percentile
    percentile: 99
    sql: ${issues_count} / ${active_user_count} ;;
  }

  # Merge requests

  dimension: merge_requests_count {
    label: "Merge Requests"
    type: number
    sql: ${TABLE}.merge_requests ;;
  }

  measure: average_merge_requests_per_user {
    group_label: "Averages"
    label: "Merge Requests per User"
    type: average
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile80_merge_requests_per_user {
    group_label: "80th Percentile Group"
    label: "Merge Requests per User"
    type: percentile
    percentile: 80
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile90_merge_requests_per_user {
    group_label: "90th Percentile Group"
    label: "Merge Requests per User"
    type: percentile
    percentile: 90
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile99_merge_requests_per_user {
    group_label: "99th Percentile Group"
    label: "Merge Requests per User"
    type: percentile
    percentile: 99
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

}
