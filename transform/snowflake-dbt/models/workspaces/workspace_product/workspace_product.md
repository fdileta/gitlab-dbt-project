{% docs mart_ping_estimations_monthly %}

Estimation model to estimate the usage for unreported self-managed instances.

{% enddocs %}

{% docs wk_rpt_event_namespace_plan_monthly %}

Captures the last plan available from mart_event_valid for a namespace during a given 
calendar month. This is the same logic used to attribute the namespace's usage for PI 
reporting (ex: common_mart_product.rpt_event_xmau_metric_monthly).

This model is intended to be JOINed to the fct_event lineage in order to determine how 
a namespace's usage will be attributed during monthly reporting.

The current month is excluded.

{% enddocs %}

