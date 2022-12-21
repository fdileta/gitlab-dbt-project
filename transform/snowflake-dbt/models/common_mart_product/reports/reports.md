{% docs rpt_event_xmau_metric_monthly %}

**Description:** GitLab.com (SaaS) xMAU metrics by month and user group (total, free, paid), sourced from the gitlab.com db replica. This model is used for xMAU/PI reporting and is the source for paid SaaS xMAU in the `[td_xmau]` snippet.

**Data Grain:**
- event_calendar_month
- user_group
- section_name
- stage_name
- group_name

**Filters Applied to Model:**
- Include events that occurred on the last 28 days of the calendar month
- Include events used in xMAU reporting (SMAU, GMAU, UMAU)
- Exclude events not associated with a user (`is_null_user = FALSE`) 
- Exclude the current month
- `Inherited` - Include valid events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Remove Events from blocked users
- `Inherited` - Rolling 24mos of Data

**Business Logic in this Model:** 
- The Last Plan Id of the Month for the Namespace is used for the `user_group` attribution
- Usage is aggregated by xMAU metric instead of `event_name`. Metrics spanning multiple events are aggregated and deduped for ease of reporting. See `event_name_array` for all events included in the metric.
- `group_name` will be NULL if the metric spans events associated with multiple groups. (This is necessary for the metrics to aggregate properly)
- `stage_name IS NULL` for UMAU events
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the installation-level Service Ping metrics by getting a 28-day count

**Other Comments:**
- The [Definitive Guide to xMAU Analysis](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/) contains additional information about xMAU reporting
- Not all stages are captured in the model. This is due to limitations in replicating Service Ping counters using the gitlab.com db Postgres replica

{% enddocs %}

{% docs rpt_event_plan_monthly %}

**Description:** GitLab.com (SaaS) usage by event, plan, and month, sourced from the gitlab.com db replica.

**Data Grain:**
- event_calendar_month
- plan_id_at_event_month
- event_name

**Filters Applied to Model:**
- Include events that occurred on the last 28 days of the calendar month
- Exclude the current month
- `Inherited` - Include valid events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Remove Events from blocked users
  - Include events where `dim_user_id IS NULL`. These do not point to a particular User, ie. 'milestones'
- `Inherited` - Rolling 24mos of Data

**Business Logic in this Model:** 
- The Last Plan Id of the Month for the Namespace is used for the `plan_id_at_event_month` attribution
- Usage is aggregated by `event_name`, meaning you cannot dedupe user or namespace counts across multiple events using this model.
  - Since some xMAU metrics aggregate across multiple events, use [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly) for xMAU reporting
- Not all events have a user associated with them (ex: 'milestones'), and not all events have a namespace associated with them (ex: 'users_created'). Therefore it is expected that `user_count = 0` or `ultimate_parent_namespace_count = 0` for these events.
- Aggregated Counts are based on the Event Date being within the Last Day of the Month and 27 days prior to the Last Day of the Month (total 28 days)
  - Events that are 29,30 or 31 days prior to the Last Day of the Month will Not be included in these totals
  - This is intended to match the installation-level Service Ping metrics by getting a 28-day count

**Tips for Use:**
- The model currently exposes a plan_id, but not a plan_name. It is recommended to JOIN to [`prep_gitlab_dotcom_plan`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_gitlab_dotcom_plan) to map the IDs to names. (Issue to add the plan_name to this model [here](https://gitlab.com/gitlab-data/analytics/-/issues/15172))

Example query to map plan_id to plan_name

```
SELECT
  event_calendar_month,
  plan_id_at_event_month,
  prep_gitlab_dotcom_plan.plan_name_modified AS plan_name_at_event_month, --plan_name_modified maps to current plan names
  event_name,
  user_count
FROM common_mart_product.rpt_event_plan_monthly
JOIN common_prep.prep_gitlab_dotcom_plan
  ON rpt_event_plan_monthly.plan_id_at_event_month = prep_gitlab_dotcom_plan.dim_plan_id
ORDER BY 1,2,3,4
;
```

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs rpt_ping_metric_first_last_versions %}

**Description:** First and last versions of GitLab that a Service Ping metric appeared on a Self-Managed installation by Edition and Prerelease. For xMAU/PI reporting, this model is used to determine the version in which a metric was introduced.
- This table provides First and Last Application Versions along with Installation Counts by Metric, Ping Edition and Prerelease.

**Data Grain:**
- metrics_path
- ping_edition
- version_is_prerelease

**Filters Applied to Model:**
- Exclude GitLab.com (SaaS) Service Pings
- Include metrics appearing on valid versions (those found in `dim_gitlab_releases`)
- `Inherited` - Include 28 Day and All-Time metrics  
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings 

**Business Logic in this Model:** 
- `First Versions` - The earliest (minimum) version found for each metrics_path, ping_edition, and version_is_prerelease
- `Last Versions` - The latest (maximum/most recent) version found for each metrics_path, ping_edition, and version_is_prerelease
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'

**Tips for Use:**
- In the _vast_ majority of use cases, pre-release versions (`version_is_prerelease = TRUE`) can add more confusion than benefit. It is highly recommended to exclude those records during analysis.
- This model can easily be joined to [`dim_ping_metric`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.dim_ping_metric) in order to get additional attributes about the metric (`time_frame`, `group_name`, `is_smau`, etc)
- This model can easily be joined to [`dim_gitlab_releases`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.dim_gitlab_releases) to get the release date for a version

Example query incorporating those tips

```
SELECT
  stage_name,
  rpt_ping_metric_first_last_versions.metrics_path,
  ping_edition,
  first_major_minor_version_with_counter,
  release_date AS first_major_minor_version_release_date
FROM common_mart_product.rpt_ping_metric_first_last_versions
JOIN common.dim_ping_metric
  ON rpt_ping_metric_first_last_versions.metrics_path = dim_ping_metric.metrics_path
JOIN common.dim_gitlab_releases
  ON rpt_ping_metric_first_last_versions.first_major_minor_version_with_counter = dim_gitlab_releases.major_minor_version
WHERE version_is_prerelease = FALSE
  AND is_smau = TRUE
ORDER BY 1,2,3
;
```

**Other Comments:**
- Metrics can be introduced on different versions for CE and EE.
- The `milestone` field of the [metrics dictionary](https://metrics.gitlab.com/) can also be used to identify the version when a metric was instrumented, but there a couple of limitations. First, many metrics are just labeled `< 13.9`, so there is a lack of more detail for older metrics. Second, since metrics can be introduced on different versions for CE and EE, `milestone` could be incorrect for one edition/distribution.
- First/last version is dependent on the metric appearing in a Service Ping payload. There are cases where this value is incorrect due to installations somehow sending the metrics from previous versions, but there is no other complete SSOT for when a metric was introduced.

{% enddocs %}

{% docs rpt_ping_latest_subscriptions_monthly %}

**Description:** Self-Managed subscriptions by month and installation (if the subscription sent a ping that month). For xMAU/PI reporting, this model is used to determine the total number of active Self-Managed subscriptions on a given month and what percent of subscriptions sent a ping from a given version. It can also be used to determine what percent of subscriptions sent a ping on a given month, etc. 
- The version an installation is reporting on (major_minor_version_id), seat count (licensed_user_count), and count of pings sent that month (ping_count) are also included
- Unpaid subscriptions (ex: OSS, EDU) are _included_ in this model

**Data Grain:**
- ping_created_date_month
- latest_subscription_id
- dim_installation_id (only populated if subscription sent a ping that month)

_Important caveat:_ The grain of this model is slightly different depending on whether a subscription sent a ping that month. It is advised to look at the `MAX()` value, grouped by `latest_subscription_id`.
- If a subscription sent a ping that month, there is 1 record per subscription per installation reporting. (Note: a subscription can be associated with > 1 installation, so a single subscription could have multiple records for a given month)
- If a subscription did not send a ping that month, there is 1 record per subscription where `dim_installation_id IS NULL`

Example query

```
WITH subscription_level AS (

  SELECT
    ping_created_date_month,
    latest_subscription_id,
    COUNT(dim_installation_id) AS installation_count,
    MAX(has_sent_pings) AS has_sent_pings,
    MAX(licensed_user_count) AS seat_count
  FROM common_mart_product.rpt_ping_latest_subscriptions_monthly
  GROUP BY 1,2

)

SELECT
  ping_created_date_month,
  COUNT(latest_subscription_id) AS subscription_count,
  COUNT(IFF(has_sent_pings = TRUE, latest_subscription_id, NULL)) AS sent_ping_count,
  DIV0(sent_ping_count, subscription_count) AS subscription_ping_opt_in_rate
FROM subscription_level
GROUP BY 1
ORDER BY 1
;
```

**Filters Applied to Model:**
- Include subscriptions where:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `major_minor_version_id`, `version_is_prerelease`, and `instance_user_count` look at 'Last Ping of the Month' pings
- Exclude the current month

**Business Logic in this Model:**
- If a ping is received from an installation with a license mapped to a subscription but no corresponding record is found in `fct_charge`, a record is still included in the model where `is_missing_charge_subscription = FALSE`. In this case, the most recent record available in `fct_charge` is used to determine the number of seats associated with the subscription.
- For a given month, all records associated with a subscription will have the same seat count (`licensed_user_count`) since that value is tied to the subscription, not an installation

{% enddocs %}

{% docs rpt_ping_subscriptions_on_versions_estimate_factors_monthly %}

**Description:** Self-Managed subscriptions and seats that sent a ping from a version of GitLab with a given metric instrumented on a given month. The totals are specific to the month, metric, edition, _and_ grain. These totals are used to generate inputs for the `metric/version check - subscription based estimation` (our "official" methodology) and `metric/version check - seat based estimation` estimation_grains for xMAU/PI reporting.

_Note: This model is not expected to be used much (if at all) for analysis. The main purpose of the model is to create inputs for the estimation lineage._

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition
- estimation_grain

**Filters Applied to Model:**
- `Inherited` - Include subscriptions where:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `Inherited` - Include metrics for 28 Day and All-Time time frames
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- There are multiple estimation grains in this model, `metric/version check - subscription based estimation` and `metric/version check - seat based estimation`
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - `metric/version check - subscription based estimation` looks at how many subscriptions sent a ping from a version of GitLab with the metric instrumented (_this is the "official" methodology used for xMAU/PI reporting_)
  - `metric/version check - seat based estimation` looks at how many seats are associated with subscriptions that sent a ping from a version of GitLab with the metric instrumented
- `percent_reporting` is defined as `reporting_count / (reporting_count + not_reporting_count)`
- `reporting_count` and `not_reporting_count` are defined by the `estimation_grain` (either count of subscriptions or count of seats)
- Subscription and seat totals are specific to the month, metric, edition, _and_ grain
- `percent_reporting`, `reporting_count`, and `not_reporting_count` are specific to the month, metric, edition, _and_ grain
- The [Self-Managed Estimation Algorithm handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html) contains more details about the estimation methodology

{% enddocs %}

{% docs rpt_ping_metric_estimate_factors_monthly %}

**Description:** The UNION of [`rpt_ping_subscriptions_on_versions_estimate_factors_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_ping_subscriptions_on_versions_estimate_factors_monthly) and [`rpt_ping_subscriptions_reported_estimate_factors_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_ping_subscriptions_reported_estimate_factors_monthly). This model contains inputs to be used in estimated uplift in the final xMAU/PI reporting model.

_Note: This model is not expected to be used much (if at all) for analysis. The main purpose of the model is to create inputs for the estimation lineage._

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition
- estimation_grain

**Filters Applied to Model:**
- `Inherited`- Include subscriptions where:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `Inherited` - Include metrics for 28 Day and All-Time time frames
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - `metric/version check - subscription based estimation` looks at how many subscriptions sent a ping from a version of GitLab with the metric instrumented (_this is the "official" methodology used for xMAU/PI reporting_)
  - `metric/version check - seat based estimation` looks at how many seats are associated with subscriptions that sent a ping from a version of GitLab with the metric instrumented
  - `reported metric - subscription based estimation` looks at how subscriptions reported the metric
  - `reported metric - seat based estimation` looks at how many seats are associated with subscriptions that reported the metric
- `percent_reporting` is defined as `reporting_count / (reporting_count + not_reporting_count)`
- `reporting_count` and `not_reporting_count` are defined by the `estimation_grain` (either count of subscriptions or count of seats)
- The [Self-Managed Estimation Algorithm handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html) contains more details about the estimation methodology

{% enddocs %}

{% docs rpt_ping_subscriptions_reported_estimate_factors_monthly %}

**Description:** Self-Managed subscriptions and seats that report a given metric on a given month. The totals are specific to the month, metric, and grain, but will be the same across editions. These totals are used to generate inputs for the `reported metric - subscription based estimation` and `reported metric - seat based estimation` estimation_grains for xMAU/PI reporting.

_Note: This model is not expected to be used much (if at all) for analysis. The main purpose of the model is to create inputs for the estimation lineage._

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition
- estimation_grain

**Filters Applied to Model:**
- `Inherited` - Include subscriptions where:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `Inherited` - Include metrics for 28 Day and All-Time time frames
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- There are multiple estimation Grains in this model, `reported metric - subscription based estimation` and `reported metric - seat based estimation`
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - `reported metric - subscription based estimation` looks at how subscriptions reported the metric
  - `reported metric - seat based estimation` looks at how many seats are associated with subscriptions that reported the metric
- `percent_reporting` is defined as `reporting_count / (reporting_count + not_reporting_count)`
- `reporting_count` and `not_reporting_count` are defined by the `estimation_grain` (either count of subscriptions or count of seats)
- For a given month, metric, and grain, `percent_reporting`, `reporting_count`, and `not_reporting_count` is the same across all editions
- The [Self-Managed Estimation Algorithm handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html) contains more details about the estimation methodology

{% enddocs %}

{% docs rpt_ping_metric_totals_w_estimates_monthly %}

**Description:** Total, recorded, and estimated usage for Self-Managed and SaaS Service Ping metrics. This model is used for xMAU/PI reporting and is the source for Service Ping data in the `[td_xmau]` snippet. You can read more about our estimation methodology on [this handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html)

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition
- estimation_grain
- ping_edition_product_tier
- ping_delivery_type

**Filters Applied to Model:**
- `Inherited` - Include metrics for 28 Day and All-Time time frames
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- `estimation_grain` - tells which method is used to measure the `percent_reporting` %:
  - `metric/version check - subscription based estimation` looks at how many subscriptions sent a ping from a version of GitLab with the metric instrumented (_this is the "official" methodology used for xMAU/PI reporting_)
  - `metric/version check - seat based estimation` looks at how many seats are associated with subscriptions that sent a ping from a version of GitLab with the metric instrumented
  - `reported metric - subscription based estimation` looks at how subscriptions reported the metric
  - `reported metric - seat based estimation` looks at how many seats are associated with subscriptions that reported the metric
  - `SaaS` looks at recorded SaaS/gitlab.com usage, there is no additional estimation logic
- `percent_reporting` is defined as `reporting_count / (reporting_count + not_reporting_count)`
- `reporting_count` and `not_reporting_count` are defined by the `estimation_grain` (either count of subscriptions or count of seats)
- For a given month, metric, delivery, edition, and grain, `percent_reporting`, `reporting_count`, and `not_reporting_count` is the same across all tiers

**Tips for Use:**
- The "official" estimation_grain is `metric/version check - subscription based estimation`
- The estimation_grain for SaaS is `SaaS`. Therefore, to pull in the values used for xMAU/PI reporting, you want to use the filter `estimation_grain IN ('metric/version check - subscription based estimation', 'SaaS')`

**Other Comments:**
- The [Definitive Guide to xMAU Analysis](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/) contains additional information about xMAU reporting
- The [Self-Managed Estimation Algorithm handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html) contains more details about the estimation methodology
- Service Ping data is Sums, Counts and Percents of Usage (called metrics) along with the Server Instance Configuration information is collected at a point in time for each Instance and sent to GitLab Corporate.  This is normally done on a weekly basis.  The Instance Owner determines whether this data will be sent or not and how much will be sent.  Implementations can be Customer Hosted (Self-Managed) or GitLab Hosted (referred to as SaaS or Dotcom data).  Multiple Instances can be hosted on Self-Managed Implementations like GitLab Implementations. 
- The different types of Service Pings are shown here for the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping) and the [GitLab Hosted Implementation Service Pings](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs rpt_ping_subscriptions_reported_counts_monthly %}

**Description:** Total Self-Managed subscriptions and seats by month. This model determines the total possible number of subscriptions on a given month and is the same across all records for a given month (there is no difference across metrics or editions). For xMAU/PI reporting, this model is used to determine the total number of active Self-Managed subscriptions and seats on a given month.

_Note: This model is not expected to be used much (if at all) for analysis. The main purpose of the model is to create inputs for the estimation lineage._

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition

**Filters Applied to Model:**
- `Inherited` - Include subscriptions where:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- For a given month, the count of subscriptions and seats is the same across every metric and edition (every record)

{% enddocs %}

{% docs rpt_ping_subscriptions_on_versions_counts_monthly %}

**Description:** Self-Managed subscriptions and seats sending a ping from a version of GitLab with the metric instrumented by month. The counts of subscriptions and seats are specific to the metric and month, but the same across editions. This model is used as an input for the `metric/version check` estimation grains in xMAU/PI reporting.

_Note: This model is not expected to be used much (if at all) for analysis. The main purpose of the model is to create inputs for the estimation lineage._

**Data Grain:**
- ping_created_date_month
- metrics_path
- ping_edition

**Filters Applied to Model:**
- `Inherited` - Subscriptions and seats are limited to:
  - `product_delivery_type = 'Self-Managed'` 
  - `subscription_status IN ('Active','Cancelled')`
  - `product_tier_name <> 'Storage'`
- `Inherited` - Include 28 Day and All-Time metrics  
- `Inherited` - Include Metrics from the 'Last Ping of the Month' pings
- `Inherited` - Exclude the current month

**Business Logic in this Model:**
- "Version of GitLab with the metric instrumented" is dependent on the first and last versions where a metric appears in a Self-Managed ping payload. These values come from [`rpt_ping_metric_first_last_versions`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_ping_metric_first_last_versions). The version is specific to both the `ping_edition` and `version_is_prerelease`
- For a given month and metric, the count of subscriptions and seats is the same across editions

{% enddocs %}

{% docs rpt_gainsight_metrics_monthly_paid_saas %}

**Description:** Joins SaaS Namespace Service Pings to a list of paid recurring SaaS subscriptions to limit to paying SaaS customers, and joins in seat/license data to calculate license utilization. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Namespace
- Subscription
- Month

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.
- Only includes recurring paid SaaS subscriptions

**Business Logic in this Model:**
- Hits Zuora tables related to charges and product rate plans to limit to paid SaaS customers with recurring subscriptions.

{% enddocs %}

{% docs rpt_gainsight_metrics_monthly_paid_self_managed %}

**Description:** Joins Self-Managed Service Pings to a list of paid Self-Managed subscriptions to limit to paying SM customers, joins in seat/license data to calculate license utilization. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Installation
- Subscription
- Month

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.
- Only includes pings that have a paid Self-Managed subscription associated with them.

**Business Logic in this Model:**
- Resolves a one-to-many relationship between installation and instance types by prioritizing production instances above other instance types
- Limits down to last ping of the month for each installation-subscription

{% enddocs %}

{% docs rpt_ping_metric_totals_w_estimates_monthly_snapshot_model %}

Simpler incremental version of the rpt_ping_metric_totals_w_estimates_monthly snapshot model. See [rpt_ping_metric_totals_w_estimates_monthly](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_ping_metric_totals_w_estimates_monthly) for more information about the model being snapshotted.

{% enddocs %}

{% docs rpt_event_xmau_metric_monthly_snapshot_model %}

Simpler incremental version of the rpt_event_xmau_metric_monthly snapshot model. See [rpt_event_xmau_metric_monthly](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly) for more information about the model being snapshotted.

{% enddocs %}
