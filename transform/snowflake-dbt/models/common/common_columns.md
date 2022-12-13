{% docs event_pk %}

The unique identifier of an event. This is a generated primary key and will not join back to the source models

{% enddocs %}

{% docs dim_active_product_tier_id %}

The unique identifier of the ultimate parent namespace's latest product tier, easily joined to `dim_product_tier`

{% enddocs %}

{% docs dim_active_subscription_id %}

The unique identifier of the ultimate parent namespace's latest subscription, easily joined to `dim_subscription`

{% enddocs %}

{% docs dim_event_date_id %}

The ID of the event date, easily joined to `dim_date`

{% enddocs %}

{% docs dim_crm_account_id %}

The unique identifier of a crm account, easily joined to `dim_crm_account`

{% enddocs %}

{% docs dim_billing_account_id %}

The identifier of the Zuora account associated with the subscription, easily joined to `dim_billing_account`

{% enddocs %}

{% docs dim_ultimate_parent_namespace_id %}

The unique identifier of the ultimate parent namespace in which the event was generated, easily joined to `dim_namespace`. The recommended JOIN is `dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id`, which will be a one-to-one relationship. JOINing on `dim_ultimate_parent_namespace_id = dim_namespace.ultimate_parent_namespace_id` will return `dim_namespace` records for both the ultimate parent _and_ all sub-groups underneath it. This field will be NULL if the event is not tied to a namespace (ex. users_created)

{% enddocs %}

{% docs dim_project_id %}

The unique identifier of the project in which the event was generated, easily joined to `dim_project`. This will be NULL if the event is not tied to a project (ex. epic_creation, etc)

{% enddocs %}

{% docs dim_user_id %}

The unique identifier of the user who generated the event, easily joined to `dim_user`. This will be NULL if the event is not tied to a specific user (ex. terraform_reports, etc)

{% enddocs %}

{% docs event_created_at %}

Timestamp of the event

{% enddocs %}

{% docs event_date %}

The date of the event

{% enddocs %}

{% docs parent_id %}

The unique identifier of the project (dim_project_id) associated with the event. If no project is associated, the ultimate parent namespace associated with the event. This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs parent_type %}

Denotes whether the event was associate with a project or namespace ('project' or 'group'). This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs is_smau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the stage's SMAU metric

{% enddocs %}

{% docs is_gmau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the group's GMAU metric

{% enddocs %}

{% docs is_umau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the UMAU metric

{% enddocs %}

{% docs event_name %}

The name tied to the event

{% enddocs %}

{% docs stage_name %}

The name of the [product stage](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/stages.yml) (ex. secure, plan, create, etc)

{% enddocs %}

{% docs section_name %}

The name of the [product section](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/sections.yml) (ex. dev, ops, etc)

{% enddocs %}

{% docs group_name %}

The name of the [product group](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/stages.yml) (ex. code_review, project_management, etc)

{% enddocs %}

{% docs product_category_ping_metric %}

The name of the [product category](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/categories.yml) (ex. audit_events, integrations, continuous_delivery, etc)

{% enddocs %}

{% docs plan_id_at_event_date %}

The ID of the ultimate parent namespace's plan on the day the event was created (ex. 34, 100, 101, etc). If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_date %}

The name of the ultimate parent namespace's plan type on the day when the event was created (ex. free, premium, ultimate). If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_date %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid on the day when the event was created. If multiple plans are available on a single day, this reflects the last available plan for the namespace. Defaults to False if a value is not available

{% enddocs %}

{% docs plan_id_at_event_timestamp %}

The ID of the ultimate parent namespace's plan at the timestamp the event was created (ex. 34, 100, 101, etc). Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_timestamp %}

The name of the ultimate parent namespace's plan type at the timestamp when the event was created (ex. free, premium, ultimate). Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_timestamp %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid at the timestamp when the event was created. Defaults to False if a value is not available

{% enddocs %}

{% docs days_since_user_creation_at_event_date %}

The count of days between user creation and the event. This will be NULL if a user is not associated with the event

{% enddocs %}

{% docs days_since_namespace_creation_at_event_date %}

The count of days between ultimate parent namespace creation and the event. This will be NULL if a namespace is not associated with the event

{% enddocs %}

{% docs days_since_project_creation_at_event_date %}

The count of days between project creation and the event. This will be NULL if a project is not associated with the event

{% enddocs %}

{% docs data_source %}

The source application where the data was extracted from (ex. GITLAB_DOTCOM, VERSION_DB)

{% enddocs %}

{% docs namespace_is_internal %}

Boolean flag set to True if the ultimate parent namespace in which the event was generated is identified as an internal GitLab namespace

{% enddocs %}

{% docs namespace_creator_is_blocked %}

Boolean flag set to True if the ultimate parent namespace creator is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs namespace_created_at %}

The timestamp of the ultimate parent namespace creation

{% enddocs %}

{% docs namespace_created_date %}

The date of the ultimate parent namespace creation

{% enddocs %}

{% docs user_created_at %}

The timestamp of the user creation

{% enddocs %}

{% docs is_blocked_user %}

Boolean flag set to True if the user who generated the events is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs project_is_learn_gitlab %}

Boolean flag set to True if the project in which the event was generated was a Learn GitLab project, one automatically created during user onboarding

{% enddocs %}

{% docs project_is_imported %}

Boolean flag set to True if the project in which the event was generated was imported

{% enddocs %}

{% docs event_calendar_month %}

The first day of the calendar month of the event (ex. 2022-05-01, etc)

{% enddocs %}

{% docs event_calendar_quarter %}

The calendar quarter of the event (ex. 2022-Q2, etc)

{% enddocs %}

{% docs event_calendar_year %}

The calendar year of the event (ex. 2022, etc)

{% enddocs %}

{% docs created_by %}

The GitLab handle of the original model creator

{% enddocs %}

{% docs updated_by %}

The GitLab handle of the most recent model editor

{% enddocs %}

{% docs model_created_date %}

Manually input ISO date of when model was original created

{% enddocs %}

{% docs model_updated_date %}

Manually input ISO date of when model was updated

{% enddocs %}

{% docs event_count %}

The count of events generated

{% enddocs %}

{% docs user_count %}

 The count of distinct users who generated an event

{% enddocs %}

{% docs ultimate_parent_namespace_count %}

 The count of distinct ultimate parent namespaces in which an event was generated

{% enddocs %}

{% docs ultimate_parent_namespace_type %}

 The type of Ultimate Parent Namespace (user,group,project)

{% enddocs %}

{% docs dim_ping_date_id %}

The ID of the Service Ping creation date, easily joined to `dim_date`

{% enddocs %}

{% docs metrics_path  %}

The unique JSON key path of the identifier of the metric in the Service Ping payload. This appears as `key_path` in the metric definition YAML files

{% enddocs %}

{% docs metric_value %}

The value associated with the metric path. In most models, metrics with a value of `-1` (those that timed out during ping generation) are set to `0`. See model description for confirmation.

{% enddocs %}

{% docs monthly_metric_value %}

For 28 day metrics, this is the metric value that comes directly from the ping payload. For all-time metrics, this is a calculation using the monthly_all_time_metric_calc macro. The macro calculates an installation-level MoM difference in metric value, attempting to create a monthly version of an all-time counter.

{% enddocs %}

{% docs has_timed_out %}

Boolean flag which is set to True if the metric timed out during Service Ping generation. In the ping payload, timed out metrics have a value of `-1`, but in most models the value is set to `0` (see model description for confirmation).

{% enddocs %}

{% docs dim_ping_instance_id %}

The unique identifier of the ping. This appears as `id` in the ping payload.

{% enddocs %}

{% docs dim_instance_id %}

The identifier of the instance, easily joined to `dim_installation`. This id is stored in the database of the installation and appears as `uuid` in the ping payload.

{% enddocs %}

{% docs dim_license_id %}

The unique identifier of the license, easily joined to `dim_license`

{% enddocs %}

{% docs dim_installation_id %}

The unique identifier of the installation, easily joined to `dim_installation`. This id is the combination of `dim_host_id` and `dim_instance_id` and is considered the unique identifier of the installation for reporting and analysis

{% enddocs %}

{% docs latest_subscription_id %}

The latest child `dim_subscription_id` of the subscription linked to the license

{% enddocs %}

{% docs dim_parent_crm_account_id %}

The identifier of the ultimate parent account, easily joined to `dim_crm_account`

{% enddocs %}

{% docs dim_host_id %}

The identifier of the host, easily joined to `dim_installation` or `dim_host`. There is a 1:1 relationship between hostname and dim_host_id, so it will be shared across installations with the same hostname.

{% enddocs %}

{% docs host_name %}

The name (URL) of the host. This appears as `hostname` in the ping payload.

{% enddocs %}

{% docs ping_delivery_type %}

How the product is delivered to the installation (Self-Managed, SaaS)

{% enddocs %}

{% docs ping_edition %}

The main edition of GitLab on the installation (EE, CE), also referred to as distribution

{% enddocs %}

{% docs ping_product_tier %}

The product tier of the ping, inferred from the edition and the plan saved in the license (Core, Starter, Premium, Ultimate). `Core` is synonymous with `Free`

{% enddocs %}

{% docs ping_edition_product_tier %}

The concatenation of `ping_edition` and `ping_product_tier` (ex. `EE - Premium`, `EE - Ultimate`, `EE - Core`, etc). `Core` is synonymous with `Free`

{% enddocs %}

{% docs major_version %}

The major version of GitLab on the installation. For example, for 13.6.2, `major_version` is 13. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs minor_version %}

The minor version of GitLab on the installation. For example, for 13.6.2, `minor_version` is 6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs major_minor_version %}

The concatenation of major and minor version, easily joined to `dim_gitlab_releases`. For example, for 13.6.2, `major_minor_version` is 13.6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html).

{% enddocs %}

{% docs major_minor_version_id %}

The id of the major minor version, defined as `major_version*100 + minor_version`. For example, for 13.6.2, the `major_minor_version_id` is 1306. This id is intended to facilitate easy filtering on versions

{% enddocs %}

{% docs version_is_prerelease %}

Boolean flag which is set to True if the version is a pre-release Version of the GitLab App. See more details [here](https://docs.gitlab.com/ee/policy/maintenance.html). This is defined as `IFF(version ILIKE '%-pre', TRUE, FALSE)`

{% enddocs %}

{% docs is_internal %}

Boolean flag set to True if the installation meets our defined "internal" criteria. However, this field seems to also capture some Self-Managed customers, so the best way to identify a gitlab.com installation is using `ping_delivery_type`

{% enddocs %}

{% docs is_staging %}

Boolean flag which is set to True if the installations meets our defined "staging" criteria (i.e., `staging` is in the host name). This is a directional identification and is not exhaustive of all staging installations

{% enddocs %}

{% docs is_trial_ping_model %}

Boolean flag which is set to True if the installation has a valid trial license at Service Ping creation. This appears as `license_trial` in the ping payload. There are cases where `is_trial` can be True even when an installation is outside of a trial period, so be cautious using this field.

{% enddocs %}

{% docs license_trial_ping_model %}

Boolean flag which is set to True if the installation has a valid trial license at Service Ping creation. There are cases where `is_trial` can be True even when an installation is outside of a trial period, so be cautious using this field.

{% enddocs %}

{% docs umau_value %}

The unique monthly active user (UMAU) value for the installation (i.e., the value of the metric flagged as UMAU)

{% enddocs %}

{% docs is_paid_gmau %}

Boolean flag set to True if the metric (Service Ping data) is chosen for the group's paid GMAU metric

{% enddocs %}

{% docs time_frame %}

The [time frame](https://docs.gitlab.com/ee/development/service_ping/metrics_dictionary.html#metric-time_frame) associated with the metric, as defined in the metric definition YAML file. May be set to `7d`, `28d`, `all`, `none`

{% enddocs %}

{% docs instance_user_count %}

The number of active users existing in the installation. In this case "active" is referring to a user's state (ex. not blocked) as opposed to an indication of user activity with the product

{% enddocs %}

{% docs original_subscription_name_slugify %}

If a subscription is linked to the license, slugified name of the subscription, easily joined to `dim_subscription`, etc

{% enddocs %}

{% docs subscription_start_month %}

The first day of the calendar month when the subscription linked to the license started

{% enddocs %}

{% docs subscription_end_month %}

The first day of the calendar month when the subscription linked to the license is supposed to end according to last agreed terms

{% enddocs %}

{% docs product_category_array %}

An array containing all of the product tier names associated associated with the subscription

{% enddocs %}

{% docs product_rate_plan_name_array %}

An array containing all of the product rate plan names associated with the subscription

{% enddocs %}

{% docs is_paid_subscription %}

Boolean flag set to True if the subscription has a MRR > 0 on the month of ping creation

{% enddocs %}

{% docs is_program_subscription %}

Boolean flag set to True if the subscription is under an EDU or OSS Program

{% enddocs %}

{% docs crm_account_name %}

The name of the crm account coming from SFDC

{% enddocs %}

{% docs parent_crm_account_name %}

The name of the ultimate parent account coming from SFDC

{% enddocs %}

{% docs parent_crm_account_billing_country %}

The billing country of the ultimate parent account coming from SFDC

{% enddocs %}

{% docs parent_crm_account_sales_segment %}

The sales segment of the ultimate parent account from SFDC. Sales Segments are explained [here](https://about.gitlab.com/handbook/sales/field-operations/gtm-resources/#segmentation)

{% enddocs %}

{% docs parent_crm_account_industry %}

The industry of the ultimate parent account from SFDC

{% enddocs %}

{% docs parent_crm_account_owner_team %}

The owner team of the ultimate parent account from SFDC

{% enddocs %}

{% docs parent_crm_account_sales_territory %}

The sales territory of the ultimate parent account from SFDC

{% enddocs %}

{% docs technical_account_manager %}

The name of the technical account manager of the CRM account

{% enddocs %}

{% docs ping_created_at %}

The timestamp when the ping was created

{% enddocs %}

{% docs ping_created_date_month %}

The first day of the calendar month when the ping was created (YYYY-MM-01)

{% enddocs %}

{% docs is_last_ping_of_month %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar month (defined by `ping_created_at`)

{% enddocs %}

{% docs collected_data_categories %}

Comma-separated list of collected data categories corresponding to the installation's settings (ex: `standard,subscription,operational,optional`)

{% enddocs %}

{% docs ping_created_date_week %}

The first day of the calendar week when the ping was created (YYYY-MM-DOW)

{% enddocs %}

{% docs is_last_ping_of_week %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar week (defined by `ping_created_at`). This field leverages `first_day_of_week` from `common.dim_date`, which defines a week as starting on Sunday and ending on Saturday.

{% enddocs %}

{% docs dim_product_tier_id_ping_model %}

The unique identifier of a product tier, easily joined to `dim_product_tier`. This will reflect the tier of the installation at time of ping creation.

{% enddocs %}

{% docs dim_subscription_id_ping_model %}

The unique identifier of a subscription, easily joined to `dim_subscription`. This is defined as the subscription_id associated with the license, with `license_subscription_id` from the ping payload as a fallback value.

{% enddocs %}

{% docs dim_location_country_id_ping_model %}

The unique identifier of a country, easily joined to `dim_location_country`. The location is associated with the IP address of the ping.

{% enddocs %}

{% docs license_md5 %}

The md5 hash of the license file.

{% enddocs %}

{% docs license_billable_users %}

The count of active users who can be billed for. Guest users and bots are not included. This value comes from the ping payload.

{% enddocs %}

{% docs historical_max_users %}

The peak active (defined as non-blocked) user count ever reported over the lifetime of the subscription.

{% enddocs %}

{% docs license_user_count %}

Count of licensed users purchased with the customer's subscription.

{% enddocs %}

{% docs dim_subscription_license_id %}

The unique identifier of a license subscription. This appears as `license_subscription_id` in the ping payload.

{% enddocs %}

{% docs is_license_mapped_to_subscription %}

Data quality boolean flag set to True if the license table has a value in both license_id and subscription_id

{% enddocs %}

{% docs is_license_subscription_id_valid %}

Data quality boolean flag set to True if the subscription_id in the license table is valid (does it exist in the subscription table?)

{% enddocs %}

{% docs is_service_ping_license_in_customerDot %}

Data quality boolean flag set to True if the license from Service Ping exist in CustomerDot.

{% enddocs %}

{% docs ping_created_date %}

The date when the ping was created (YYYY-MM-DD)

{% enddocs %}

{% docs ping_created_date_28_days_earlier %}

The date 28 days prior to when the ping was created

{% enddocs %}

{% docs ping_created_date_year %}

The year when the ping was created (YYYY-01-01)

{% enddocs %}

{% docs ip_address_hash_ping_model %}

The hashed source_ip associated with the ping

{% enddocs %}

{% docs recorded_at_ping_model %}

The time when the Service Ping computation was started

{% enddocs %}

{% docs updated_at_ping_model %}

The time when the ping data was last updated from the Versions db

{% enddocs %}

{% docs source_license_id %}

The unique identifier of the source license. This appears as `license_id` in the ping payload.

{% enddocs %}

{% docs license_starts_at %}

The date the license starts

{% enddocs %}

{% docs license_expires_at %}

The date the license expires

{% enddocs %}

{% docs license_add_ons %}

The add-ons associated with the license

{% enddocs %}

{% docs version_ping_model %}

The full version of GitLab associated with the installation (ex. 13.8.8-ee, 15.4.2, etc). See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs cleaned_version %}

The full version of GitLab associated with the installation, formatted as `(Major).(Minor).(Patch)` (ex. 13.8.8, 15.4.2, 14.7.0). See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs mattermost_enabled %}

Boolean flag set to True if Mattermost is enabled

{% enddocs %}

{% docs installation_type %}

The type of installation associated with the instance (i.e. gitlab-development-kit, gitlab-helm-chart, gitlab-omnibus-helm-chart)

{% enddocs %}

{% docs license_plan %}

The license plan associated with the installation (ex, premium, ultimate). This value comes directly from the ping payload

{% enddocs %}

{% docs uuid_ping_model %}

The identifier of the instance. This value is synonymous with `dim_instance_id` in other models.

{% enddocs %}

{% docs host_id %}

The identifier of the host. There is a 1:1 relationship between hostname and host_id, so it will be shared across installations with the same hostname. This value is synonymous with `dim_host_id` in other models

{% enddocs %}

{% docs id_ping_model %}

The unique identifier for a Service Ping. This value is synonymous with `dim_ping_instance_id` in other models.

{% enddocs %}

{% docs original_edition %}

The unmodified `edition` value as it appears in the ping payload (ex. CE, EE, EES, EEP, EEU, EE Free)

{% enddocs %}

{% docs cleaned_edition %}

A modified version of the edition of GitLab on the installation, with 3 possible values: CE, EE, and EE Free. Here is the SQL that generates this value

`IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, main_edition, 'EE Free')`

{% enddocs %}

{% docs database_adapter %}

The database adapter associated with the installation. This only returns a value of `postgresql` in supported versions of GitLab.

{% enddocs %}

{% docs database_version %}

The version of the PostgreSQL database associated with the installation (ex. 9.5.3, 12.10, etc)

{% enddocs %}

{% docs git_version %}

The version of Git the installations is running (ex. 2.29.0, 2.35.1., 2.14.3, etc)

{% enddocs %}

{% docs gitlab_pages_enabled %}

Boolean flag set to True if GitLab Pages is enabled

{% enddocs %}

{% docs gitlab_pages_version %}

The version number of GitLab Pages (ex. 1.25.0, 1.51.0)

{% enddocs %}

{% docs container_registry_enabled %}

Boolean flag set to True if container registry is enabled

{% enddocs %}

{% docs elasticsearch_enabled %}

Boolean flag set to True if Elasticsearch is enabled

{% enddocs %}

{% docs geo_enabled %}

Boolean flag set to True if Geo is enabled

{% enddocs %}

{% docs gitlab_shared_runners_enabled %}

Boolean flag set to True if shared runners is enabled

{% enddocs %}

{% docs gravatar_enabled %}


Boolean flag set to True if Gravatar is enabled
{% enddocs %}

{% docs ldap_enabled %}

Boolean flag set to True if LDAP is enabled

{% enddocs %}

{% docs omniauth_enabled %}

Boolean flag set to True if OmniAuth is enabled

{% enddocs %}

{% docs reply_by_email_enabled %}

Boolean flag set to True if incoming email is set up

{% enddocs %}

{% docs signup_enabled %}

Boolean flag set to True if public signup (aka "Open Registration") is enabled. More details about this feature [here](https://gitlab.com/groups/gitlab-org/-/epics/4214)

{% enddocs %}

{% docs prometheus_metrics_enabled %}

Boolean flag set to True if the Prometheus Metrics endpoint is enabled

{% enddocs %}

{% docs usage_activity_by_stage %}

JSON object containing the `usage_activity_by_stage` metrics

{% enddocs %}

{% docs usage_activity_by_stage_monthly %}

JSON object containing the `usage_activity_by_stage_monthly` metrics

{% enddocs %}

{% docs gitaly_clusters %}

Count of total GitLab Managed clusters, both enabled and disabled

{% enddocs %}

{% docs gitaly_version %}

Version of Gitaly running on the installation (ex. 14.2.1, 15.5.1, etc)

{% enddocs %}

{% docs gitaly_servers %}

Count of total Gitaly servers

{% enddocs %}

{% docs gitaly_filesystems %}

Filesystem data for Gitaly installations

{% enddocs %}

{% docs gitpod_enabled %}

Text flag set to True if Gitpod is enabled. This is not a boolean field, so values are `t` and `f` instead of `TRUE` and `FALSE`

{% enddocs %}

{% docs object_store %}

JSON object containing the `object_store` metrics

{% enddocs %}

{% docs is_dependency_proxy_enabled %}

Boolean flag set to True if the dependency proxy is enabled

{% enddocs %}

{% docs recording_ce_finished_at %}

The time when CE features were computed

{% enddocs %}

{% docs recording_ee_finished_at %}

The time with EE-specific features were computed

{% enddocs %}

{% docs is_ingress_modsecurity_enabled %}

Boolean flag set to True if ModSecurity is enabled within Ingress

{% enddocs %}

{% docs topology %}

JSON object containing the `topology` metrics

{% enddocs %}

{% docs is_grafana_link_enabled %}

Boolean flag set to True if Grafana is enabled

{% enddocs %}

{% docs analytics_unique_visits %}

JSON object containing the `analytics_unique_visits` metrics

{% enddocs %}

{% docs raw_usage_data_id %}

The unique identifier of the raw usage data in the Versions db

{% enddocs %}

{% docs container_registry_vendor %}

The vendor of the container registry (ex. gitlab)

{% enddocs %}

{% docs container_registry_version %}

The version of the container registry in use (ex. 2.11.0-gitlab, 3.60.1-gitlab, etc)

{% enddocs %}

{% docs is_saas_dedicated %}

Boolean flag set to True if the ping is from a Dedicated implementation

{% enddocs %}

{% docs raw_usage_data_payload %}

Either the original payload value or a reconstructed value. See https://gitlab.com/gitlab-data/analytics/-/merge_requests/4064/diffs#bc1d7221ae33626053b22854f3ecbbfff3ffe633 for rationale.

{% enddocs %}

{% docs license_sha256 %}

The SHA-256 hash of the license file.

{% enddocs %}

{% docs stats_used %}

JSON object containing the `stats` metrics

{% enddocs %}

{% docs license_trial_ends_on %}

The date the trial license ends

{% enddocs %}

{% docs license_subscription_id %}

The unique identifier of a license subscription. This value is synonymous with `dim_subscription_license_id` in other models

{% enddocs %}

{% docs milestone_ping_metric %}

The milestone when the metric was introduced and when it was available to Self-Managed installations with the official GitLab release

{% enddocs %}

{% docs metrics_status_ping_metric %}

[Status](https://docs.gitlab.com/ee/development/service_ping/metrics_dictionary.html#metric-statuses) of the metric, may be set to `active`, `removed`, or `broken`.

{% enddocs %}

{% docs data_source_ping_metric %}

The source of the metric. May be set to a value like `database`, `redis`, `redis_hll`, `prometheus`, `system`.

{% enddocs %}

{% docs dim_crm_task_sk %}

The unique surrogate key of a [task activity](https://help.salesforce.com/s/articleView?id=sf.tasks.htm&type=5) related to Salesforce account, opportunity, lead, or contact.

{% enddocs %}

{% docs snapshot_id %}

The ID of the date the snapshot was valid, easily joined to `dim_date` (YYYYMMDD)

{% enddocs %}

{% docs snapshot_date %}

The date the snapshot record was valid (YYYY-MM-DD)

{% enddocs %}

{% docs dbt_scd_id %}

A unique key generated for each snapshotted record. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_created_at_snapshot_model %}

The created_at timestamp of the source record when this snapshot row was inserted. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_updated_at_snapshot_model %}

The updated_at timestamp of the source record when this snapshot row was inserted. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_valid_from %}

The timestamp when this snapshot row was first inserted. This column can be used to order the different "versions" of a record.

{% enddocs %}

{% docs dbt_valid_to %}

The timestamp when this row became invalidated. The most recent snapshot record will have `dbt_valid_to` set to null.

{% enddocs %}
