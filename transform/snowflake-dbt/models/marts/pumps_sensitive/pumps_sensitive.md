{% docs pump_marketing_contact %}

A copy of mart_marketing_contact for sending to Marketo for use in email campaigns. New user cohorts should be added by creating a flag in mart_marketing_contact and then adding the flag to this pump_marketing_contact model.

User Cohorts Included:

PQL Users via is_pql = TRUE flag in mart_marketing_contact.

{% enddocs %}

{% docs pump_subscription_product_usage %}

A copy of `subscription_product_usage_data` model for sending to Salesforce

{% enddocs %}

{% docs pump_product_usage_free_user_metrics_monthly %}
This table pulls in data from `mart_product_usage_free_users_metrics_monthly` which contains the sets of all Self-Managed and SaaS **free users**. The data from this table  pumped to Salesforce will be used for Customer Product Insights.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs pump_disaster_relief_fund %}
This table pulls data from the employee directory and populated a list of employee hires and terminations to be used by an external partner to validate request for disaster relief funds.

{% enddocs %}

{% docs pump_daily_data_science_scores %}
This table contains data science generated scores that will be uploaded to Salesforce.
{% enddocs %}