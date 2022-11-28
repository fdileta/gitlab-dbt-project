{% docs mart_ci_runner_activity_monthly %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a monthly grain per `dim_namespace_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes.

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used for  Customer Product Insights by Sales team.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_ci_runner_activity_daily %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a daily grain per `dim_project_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes.

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_ping_instance_metric_health_score_self_managed %}

**Description:** Joins together facts and dimensions related to Self-Managed Service Pings, and does a simple aggregation to pivot out and standardize metric values. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Service Ping Payload

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.

{% enddocs %}

{% docs mart_ping_namespace_metric_health_score_saas %}

**Description:** Joins together facts and dimensions related to SaaS Namespace Service Pings, and does a simple aggregation to pivot out and standardize metric values. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Namespace
- Subscription
- Month

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.

**Business Logic in this Model:**
- Resolves a one-to-many relationship between namespaces and instance types by prioritizing production instances above other instance types
- Limits down to last ping of the month for each namespace-subscription
- Currently, bridges uses the order to bridge from subscription to namespace. In the future, we will use Zuora subscription tables to get the namespace directly from the subscription.

{% enddocs %}
