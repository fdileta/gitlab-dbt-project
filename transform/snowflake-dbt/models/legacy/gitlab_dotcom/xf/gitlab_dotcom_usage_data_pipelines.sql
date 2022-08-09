{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
        "materialized": "incremental",
        "unique_key": "event_primary_key",
        "automatic_clustering": true
    })
}}

/*
  Each dict must have ALL of the following:
    * event_name
    * primary_key
    * stage_name": "create",
    * "is_representative_of_stage
    * primary_key"
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project OR key_to_group_project (NOT both, see how clusters_applications_helm is included twice for group and project.
*/

{%- set event_ctes = [
  {
    "event_name": "action_monthly_active_users_project_repo",
    "source_cte_name": "action_monthly_active_users_project_repo_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "create",
    "is_representative_of_stage": "True"
  }, {
    "event_name": "action_monthly_active_users_design_management",
    "source_cte_name": "action_monthly_active_users_design_management_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  }, {
    "event_name": "action_monthly_active_users_wiki_repo",
    "source_cte_name": "action_monthly_active_users_wiki_repo_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "api_fuzzing",
    "source_cte_name": "api_fuzzing_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "boards",
    "source_table_name": "gitlab_dotcom_boards",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "board_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "successful_ci_pipelines",
    "source_cte_name": "successful_ci_pipelines_source",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_pipeline_id",
    "stage_name": "verify",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "clusters_applications_helm",
    "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "user_column_name": "user_id",
    "key_to_parent_project": "cluster_project_id",
    "primary_key": "clusters_applications_helm_id",
    "stage_name": "configure",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "container_scanning",
    "source_cte_name": "container_scanning_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "protect",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "dast",
    "source_cte_name": "dast_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "dependency_scanning",
    "source_cte_name": "dependency_scanning_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "deployments",
    "source_table_name": "gitlab_dotcom_deployments",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "deployment_id",
    "stage_name": "release",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "environments",
    "source_table_name": "gitlab_dotcom_environments",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "environment_id",
    "stage_name": "release",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "epics",
    "source_table_name": "gitlab_dotcom_epics",
    "user_column_name": "author_id",
    "key_to_parent_group": "group_id",
    "primary_key": "epic_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "labels",
    "source_table_name": "gitlab_dotcom_labels",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "label_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "license_scanning",
    "source_cte_name": "license_scanning_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "merge_requests",
    "source_table_name": "gitlab_dotcom_merge_requests",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "merge_request_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "milestones",
    "source_table_name": "gitlab_dotcom_milestones",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "milestone_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "packages",
    "source_table_name": "gitlab_dotcom_packages_packages",
    "user_column_name": "creator_id",
    "key_to_parent_project": "project_id",
    "primary_key": "packages_package_id",
    "stage_name": "package",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "project_auto_devops",
    "source_table_name": "gitlab_dotcom_project_auto_devops",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "project_auto_devops_id",
    "stage_name": "configure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "projects_container_registry_enabled",
    "source_cte_name": "projects_container_registry_enabled_source",
    "user_column_name": "creator_id",
    "key_to_parent_project": "project_id",
    "primary_key": "project_id",
    "stage_name": "package",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "projects_prometheus_active",
    "source_cte_name": "projects_prometheus_active_source",
    "user_column_name": "creator_id",
    "key_to_parent_project": "project_id",
    "primary_key": "project_id",
    "stage_name": "monitor",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "releases",
    "source_table_name": "gitlab_dotcom_releases",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "release_id",
    "stage_name": "release",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "requirements",
    "source_table_name": "gitlab_dotcom_requirements",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "requirement_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "sast",
    "source_cte_name": "sast_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "secret_detection",
    "source_cte_name": "secret_detection_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "secure_stage_ci_jobs",
    "source_table_name": "gitlab_dotcom_secure_stage_ci_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "secure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "services",
    "source_cte_name": "services_source",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "service_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "snippets",
    "source_table_name": "gitlab_dotcom_snippets",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "snippet_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "terraform_reports",
    "source_cte_name": "terraform_reports_source",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_job_artifact_id",
    "stage_name": "configure",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "todos",
    "source_table_name": "gitlab_dotcom_todos",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "todo_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "users",
    "source_table_name": "gitlab_dotcom_users",
    "user_column_name": "user_id",
    "primary_key": "user_id",
    "stage_name": "manage",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "push_events",
    "source_cte_name": "push_events_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_pipelines",
    "source_table_name": "gitlab_dotcom_ci_pipelines",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_pipeline_id",
    "stage_name": "verify",
    "is_representative_of_stage": "True"
  },
]
-%}


{{ simple_cte([
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('namespaces', 'gitlab_dotcom_namespaces_xf'),
    ('plans', 'gitlab_dotcom_plans'),
    ('projects', 'gitlab_dotcom_projects_xf'),
    ('blocked_users', 'gitlab_dotcom_users_blocked_xf'),
    ('user_details','gitlab_dotcom_users')
]) }}


/* Source CTEs Start Here */
, action_monthly_active_users_project_repo_source AS (

    SELECT *
    FROM  {{ ref('temp_gitlab_dotcom_events_filtered') }}
    WHERE target_type IS NULL
      AND event_action_type_id = 5
), action_monthly_active_users_design_management_source AS (

    SELECT *
    FROM  {{ ref('temp_gitlab_dotcom_events_filtered') }}
    WHERE target_type = 'DesignManagement::Design'
      AND event_action_type_id IN (1, 2)

), action_monthly_active_users_wiki_repo_source AS (

    SELECT *
    FROM  {{ ref('temp_gitlab_dotcom_events_filtered') }}
    WHERE target_type = 'WikiPage::Meta'
      AND event_action_type_id IN (1, 2)

), api_fuzzing_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'api_fuzzing'

), container_scanning_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'container_scanning'

), dast_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'dast'

), dependency_scanning_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'dependency_scanning'

), license_scanning_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type IN (
                                  'license_scanning',
                                  'license_management'
                                )

), projects_prometheus_active_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
    WHERE ARRAY_CONTAINS('PrometheusService'::VARIANT, active_service_types)

), projects_container_registry_enabled_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
    WHERE container_registry_enabled = True

), sast_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'sast'

), secret_detection_jobs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_secure_stage_ci_jobs') }}
    WHERE secure_ci_job_type = 'secret_detection'

), services_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_integrations') }}
    WHERE service_type != 'GitlabIssueTrackerService'

), successful_ci_pipelines_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_pipelines') }}
    WHERE failure_reason IS NULL

), terraform_reports_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_job_artifacts') }}
    WHERE file_type = 18

), push_events_source AS (

    SELECT *
    FROM {{ ref('temp_gitlab_dotcom_events_filtered') }}
    WHERE event_action_type = 'pushed'

)
/* End of Source CTEs */

{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (

    SELECT *,
      MD5({{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}')   AS event_primary_key
    /* Check for source_table_name, else use source_cte_name. */
    {% if event_cte.source_table_name is defined %}
      FROM {{ ref(event_cte.source_table_name) }}
    {% else %}
      FROM {{ event_cte.source_cte_name }}
    {% endif %}
    WHERE created_at IS NOT NULL
      AND created_at >= DATEADD(MONTH, -25, CURRENT_DATE)
      
    {% if is_incremental() %}

      AND created_at > (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')

    {% endif %}

)

{% endfor -%}

, data AS (

{% for event_cte in event_ctes %}

    SELECT 
      event_primary_key,
      '{{ event_cte.event_name }}' AS event_name,
      {{ event_cte.user_column_name }} AS user_id,
      created_at AS event_created_at,
      {{ event_cte.is_representative_of_stage }}::BOOLEAN AS is_representative_of_stage,
      '{{ event_cte.stage_name }}' AS stage_name,
      {% if event_cte.key_to_parent_project is defined -%}

      {{ event_cte.key_to_parent_project }} 
 
      {%- elif event_cte.key_to_parent_group is defined -%}

      {{ event_cte.key_to_parent_group }} 
      
      {%- else -%}
      NULL 
      {%- endif %}::NUMBER AS parent_id,
      {% if event_cte.key_to_parent_project is defined -%}
        'project'
       
      {%- elif event_cte.key_to_parent_group is defined -%}
        'group'
  
      {%- else -%}
        NULL     
      {%- endif %} AS parent_type
    FROM {{ event_cte.event_name }}                                                                              

    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {% endfor -%}

),

joins AS (
  SELECT
    data.event_primary_key,
    data.event_name,
    ultimate_namespace.namespace_id,
    ultimate_namespace.namespace_created_at,
    IFF(blocked_users.user_id IS NOT NULL, TRUE, FALSE) AS is_blocked_namespace,
    data.user_id,
    data.parent_type,
    data.parent_id,
    COALESCE(projects.project_created_at,namespaces.namespace_created_at) AS parent_created_at,
    projects.is_learn_gitlab AS project_is_learn_gitlab,
    ultimate_namespace.namespace_is_internal AS namespace_is_internal,
    data.event_created_at,
    data.is_representative_of_stage,
    data.stage_name,
    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END AS plan_id_at_event_date,
    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(plans.plan_name, 'free')
    END AS plan_name_at_event_date,
    COALESCE(plans.plan_is_paid, FALSE) AS plan_was_paid_at_event_date
  FROM data
  /* Join with parent project. */

      LEFT JOIN projects
        ON data.parent_id = projects.project_id
        AND data.parent_type = 'project'
      /* Join with parent group. */
      LEFT JOIN namespaces
        ON data.parent_id = namespaces.namespace_id
        AND data.parent_type = 'group'

      -- Join on either the project's or the group's ultimate namespace.
      LEFT JOIN namespaces AS ultimate_namespace

        ON ultimate_namespace.namespace_id = COALESCE(projects.ultimate_parent_id,namespaces.namespace_ultimate_parent_id)


      LEFT JOIN gitlab_subscriptions
        ON ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
        AND data.event_created_at >= TO_DATE(gitlab_subscriptions.valid_from)
        AND data.event_created_at < {{ coalesce_to_infinity("TO_DATE(gitlab_subscriptions.valid_to)") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
      LEFT JOIN blocked_users
        ON ultimate_namespace.creator_id = blocked_users.user_id 
      WHERE {{ filter_out_blocked_users('data' , 'user_id') }}
      


)
, final AS (
    SELECT
      joins.*,
      user_details.created_at                                    AS user_created_at,
      FLOOR(
      DATEDIFF('hour',
              namespace_created_at,
              event_created_at)/24)                       AS days_since_namespace_creation,
      FLOOR(
        DATEDIFF('hour',
                namespace_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_namespace_creation,
      FLOOR(
        DATEDIFF('hour',
                parent_created_at,
                event_created_at)/24)                     AS days_since_parent_creation,
      FLOOR(
        DATEDIFF('hour',
                parent_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_parent_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/24)                     AS days_since_user_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_user_creation
    FROM joins
    LEFT JOIN user_details
      ON joins.user_id = user_details.user_id
    WHERE event_created_at < CURRENT_DATE()

)

SELECT *
FROM final
