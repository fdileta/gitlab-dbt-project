{% docs subscription_product_usage_data %}

This model collates a variety of product usage data metrics at the subscription_id granularity for both self-managed and SaaS subscriptions. Detailed documentation on the creation of this model, constraints, and example queries can be found on the [Master Subscription Product Usage Data Process Dashboard](https://app.periscopedata.com/app/gitlab/686439/Master-Subscription-Product-Usage-Data-Process).

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_monthly_diff %}

The purpose of this mart table is to act as a data pump of the `_all_time_event` Usage Ping metrics at a _monthly grain_ into Gainsight for Customer Product Insights (`_all_time_user` columns are not included). To accomplish this goal, this model includes a column that takes the _diff_ erences in `_all_time_event` values between consecutive monthly Usage Pings. Since some months do not contain Usage Ping data, these _diff_ erences are normalized (estimated) to a monthly value based on the average daily value over the time between pings multiplied by the days in the calendar month(s) between the consecutive pings.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_product_usage_paid_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **paid users**. The data from this table will be used for Customer Product Insights.

The grain of this table is subscription per namespace || uuid-hostname per month.

The join to `dim_subscription_snapshot_bottom_up` uses a datediff of -1 day so that the `subscription_status` reflects the position at the end of the previous month. This avoids the situation where a subscription expires on the last day of the month and new one begins on the 1st of the next month meaning the join produces a NULL.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs pump_gainsight_metrics_monthly_paid %}

**Description:** This table unions together a select set of Service Ping metrics for both Self-Managed and SaaS **paid users**. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Subscription (`dim_subscription_id_original`)
- Month (`snapshot_month`)
- Delivery Type (`delivery_type`)
- Installation/Namespace (For Self-Managed, `uuid`-`hostname`; for SaaS, `namespace_id`)

**Filters:**
Inherits filters from parent models, but most notably:
  - Only includes paid customers.
  - Only includes Service Ping metrics that have been added via the "wave" process.
  - Only includes subscriptions that have a usage ping payload associated with them.

**Other Comments:**
- For Self-Managed customers, this data orgininates in [Service Ping](https://docs.gitlab.com/ee/development/service_ping/), which sends a weekly payload of customer product usage metrics to GitLab. For SaaS customers, we mimic the Service Ping queries using the [SaaS Service Ping process](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/). Specifically, the namespace-level Service Ping queries can be found [here](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/saas_usage_ping/usage_ping_namespace_queries.json).
- For SaaS customers, not all metrics can be calculated via the namespace-level Service Ping. For metrics that originate from `redis_hll`, Snowplow counters are used to track event-level data. Then, the data team aggregates those counters in Snowflake to mimic the Service Ping calcuation.

{% enddocs %}
