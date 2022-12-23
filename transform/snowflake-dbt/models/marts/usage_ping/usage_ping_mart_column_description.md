{% docs reporting_month %}
Month of Reporting
{% enddocs %}
{% docs ping_id%}
unique identifier of a ping
{% enddocs %}
{% docs license_id%}
Unique Identifier of a license
{% enddocs %}
{% docs original_linked_subscription_id%}
if not null, the subscription ID currently linked to the license ID
{% enddocs %}
{% docs latest_active_subscription_id%}
If not null, the latest child subscription ID of the subscription linked to the license
{% enddocs %}
{% docs billing_account_id%}
The ID of the Zuora account the subscription is associated with
{% enddocs %}
{% docs location_id%}
{% enddocs %}
{% docs delivery_column %}
Either Self-Managed or SaaS. More info here: https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/
{% enddocs %}
{% docs main_edition%}
EE vs CE. More info here: https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/
{% enddocs %}
{% docs ping_main_edition_product_tier%}
Concatenation of main_edition and ping_product_tier
{% enddocs %}
{% docs version%}
Version of the GitLab App. See more details here: https://docs.gitlab.com/ee/policy/maintenance.html
{% enddocs %}
{% docs is_pre_release%}
Boolean flag which is set to True if the version is a pre-release Version of the GitLab App. See more details here: https://docs.gitlab.com/ee/policy/maintenance.html
{% enddocs %}
{% docs source_ip_hash%}
{% enddocs %}
{% docs subscription_name_slugify%}
If a subscription is linked to the license, slugified name of the subscription
{% enddocs %}
{% docs is_edu_oss_subscription%}
Boolean flag set to True if the subscription is under a EDU/OSS Program
{% enddocs %}
{% docs created_at%}
Timestamp when the usage ping payloads has been created
{% enddocs %}
{% docs recorded_at%}
Timestamp when the usage ping payloads has been recorded
{% enddocs %}