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

The name of the product stage (ex. secure, plan, create, etc)

{% enddocs %}

{% docs section_name %}

The name of the product section (ex. dev, ops, etc)

{% enddocs %}

{% docs group_name %}

The name of the product group (ex. code_review, project_management, etc)

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

The source application where the data was extracted from (ex. GITLAB_DOTCOM)

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

The unique JSON key path of identifier of the metric in the Service Ping payload. This appears as `key_path` in the metric definition YAML files

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

The identifier of the instance, easily joined to `dim_installation`. This id is store in the database of the installation and appears as `uuid` in the ping payload.

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

The edition of GitLab on the installation (EE, CE), also referred to as distribution

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

The minor version of GitLab on the instance. For example, for 13.6.2, `minor_version` is 6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs major_minor_version %}

The concatenation of major and minor version. For example, for 13.6.2, `major_minor_version` is 13.6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs major_minor_version_id %}

The id of the major minor version, defined as `major_version*100 + minor_version`. For example, for 13.6.2, the `major_minor_version_id` is 1306. This id is intended to facilitate easy filtering on versions

{% enddocs %}

{% docs version_is_prerelease %}

Boolean flag which is set to True if the version is a pre-release Version of the GitLab App. See more details here (https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs is_internal %}

Boolean flag set to True if the installation meets our defined "internal" criteria. However, this field seems to also capture some Self-Managed customers, so the best way to identify a gitlab.com installation is using `ping_delivery_type`

{% enddocs %}

{% docs is_staging %}

Boolean flag which is set to True if the installations meets our defined "staging" criteria (i.e., `staging` is in the host name). This is a directional identification and is not exhaustive of all staging installations

{% enddocs %}

{% docs is_trial %}

Boolean flag which is set to True if the installation has a valid trial license at Service Ping creation. There are cases where `is_trial` can be True even when an installation is outside of a trial period

{% enddocs %}

{% docs umau_value %}

The unique monthly active user value for the installation (i.e., the value of the metric flagged as UMAU)

{% enddocs %}

{% docs is_paid_gmau %}

Boolean flag set to True if the metric (Service Ping data) is chosen for the group's paid GMAU metric

{% enddocs %}

{% docs time_frame %}

The time frame associated with the metric, as defined in the metric definition YAML file (ex. 28d, all, etc)

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

The name of the technical account manager of the subscription

{% enddocs %}

{% docs ping_created_at %}

The timestamp when the ping was created

{% enddocs %}

{% docs ping_created_date_month %}

The first day of the calendar month when the ping was created

{% enddocs %}

{% docs is_last_ping_of_month %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar month (defined by `ping_created_at`)

{% enddocs %}

{% docs collected_data_categories %}

Comma-separated list of collected data categories corresponding to the installation's settings (ex: `standard,subscription,operational,optional`)

{% enddocs %}

{% docs ping_created_date_month %}

The first day of the calendar week when the ping was created

{% enddocs %}

{% docs is_last_ping_of_week %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar week (defined by `ping_created_at`). This field leverages `first_day_of_week` from `common.dim_date`, which defines a week as starting on Sunday and ending on Saturday.

{% enddocs %}
