{% docs bdg_crm_opportunity_contact_role %}

A fact table bridging opportunities with contacts. One opportunity can have multiple contacts and one can be flagged as the primary.

{% enddocs %}

{% docs bdg_epic_user_request %}

A bridge table that connects epics user requests, these epics being in the `Gitlab-org` group, with SFDC Opportunities / Accounts and Zendesk tickets links. It also picks the priorities that have been assigned to the epic request.

This table combines the requests that were done directly in the `Gitlab-org` group by pasting the SFDC / Zendesk links directly in the epic description / notes (`prep_epic_user_request`), with the requests that were done by pasting the epic links in the customer collaboration projects (`prep_epic_user_request_collaboration_project`). If the combination of epic and link is found in both the `Gitlab-org` group and the customer collaboration project, the `Gitlab-org` will take precedence. If the request is only in the customer collaboration project then the flag `is_user_request_only_in_collaboration_project` will be equal to `True`.

{% enddocs %}

{% docs bdg_issue_user_request %}

A bridge table that connects issues user requests, these issues being in the `Gitlab-org` group, with SFDC Opportunities / Accounts and Zendesk tickets links. It also picks the priorities that have been assigned to the issue request.

This table combines the requests that were done directly in the `Gitlab-org` group by pasting the SFDC / Zendesk links directly in the issue description / notes (`prep_issue_user_request`), with the requests that were done by pasting the issue links in the customer collaboration projects (`prep_issue_user_request_collaboration_project`). If the combination of issue and link is found in both the `Gitlab-org` group and the customer collaboration project, the `Gitlab-org` will take precedence. If the request is only in the customer collaboration project then the flag `is_user_request_only_in_collaboration_project` will be equal to `True`.

{% enddocs %}

{% docs bdg_namespace_order_subscription_monthly %}

The purpose of this table is three-fold:
1. Connect **Ultimate Parent** Namespace ID to Subscription (and hence Zuora billing account and CRM Account)
2. Connect Customer DB Customer ID to Subscription for self managed purchases. This helps with marketing efforts.
3. Provide a historical record the above connections by month.

This table expands the functionality of the orders by improving the join to ultimate parent namespaces and subscriptions. Namespaces are listed in this table with prior trials and currently paid plans. Subscriptions listed in this table are all SaaS (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all SaaS (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three sides of the Venn diagram. (namespace, orders, subscriptions)
In doing so exceptions are noted within `namespace_order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_namespace_order_subscription %}

The purpose of this table is two-fold:
1. Connect **Ultimate Parent** Namespace ID to Subscription (and hence Zuora billing account and CRM Account)
2. Connect Customer DB Customer ID to Subscription for self managed purchases. This helps with marketing efforts.

This table expands the functionality of the orders by improving the join to ultimate parent namespaces and subscriptions. Namespaces are listed in this table with prior trials and currently paid plans. Subscriptions listed in this table are all SaaS (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all SaaS (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three sides of the Venn diagram. (namespace, orders, subscriptions)
In doing so exceptions are noted within `namespace_order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_self_managed_order_subscription %}

The purpose of this table to connect Order IDs from Customer DB to Subscription for Self-Managed purchases. This table expands the functionality of the subscriptions by improving the join to orders. Subscriptions listed in this table are all Self-Managed (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all Self-Managed (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three parts of the Venn diagram (orders, subscriptions, and the overlap between the two).In doing so exceptions are noted within `order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_subscription_product_rate_plan %}
The goal of this table is to build a bridge from the entire "universe" of subscriptions in Zuora (`zuora_subscription_source` without any filters applied) to all of the [product rate plans](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan) to which those subscriptions are mapped. This provides the ability to filter subscriptions by delivery type ('SaaS' or 'Self-Managed').

{% enddocs %}

{% docs dim_accounting_event %}

Events from Zuora Revpro. The current iteration includes performance obligation events, but will eventually include hold events as well.

{% enddocs %}

{% docs dim_accounting_type %}

Model to map revenue from Zuora Revenue to the appropriate account (revenue, contract liability, etc.) per accounting practices.

{% enddocs %}

{% docs dim_alliance_type_scd %}
[Slowly changing dimension type 2](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/#:~:text=Slowly%20changing%20dimension%20type%202,multiple%20rows%20describing%20each%20member.) to identify Channel partners groupings. Can be joined to either `dim_alliance_type_id` to get the historical information on channel partners or to `dim_alliance_type_current_id` to get the most recent state of channel partners.

Technology Partners are identified and discussed in the handbook link referenced below. The specific groupings to report out on were determined by FP&A and Sales Analytics.

[Technology Partners Handbook Reference](https://about.gitlab.com/handbook/alliances/#technology-partners)

{% enddocs %}

{% docs dim_crm_account %}
Dimensional customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as `DIM_CRM_ACCOUNT_ID`.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_crm_account_daily_snapshot %}
Dimensional customer table representing all existing and historical customers from SalesForce and their attributes on a given day. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account and day, also referred to as `CRM_ACCOUNT_SNAPSHOT_ID`, which is a combination of the `DIM_CRM_ACCOUNT_ID` and `DIM_DATE_ID`

{% enddocs %}

{% docs dim_crm_touchpoint %}
Descriptive fields for both attribution and non-attribution Bizible touchpoints.

{% enddocs %}

{% docs dim_crm_opportunity %}
Model for all dimensional opportunity columns from salesforce opportunity object

{% enddocs %}

{% docs dim_crm_person %}
Dimension that combines demographic data from salesforce leads and salesforce contacts. They are combined with a union and a filter on leads excluding converted leads and leads where there is a corresponding contact.

{% enddocs %}

{% docs dim_crm_user %}

Dimension representing the associated user from salesforce. Most often this will be the record owner, which is a ubiquitous field in salesforce.

{% enddocs %}

{% docs dim_crm_user_daily_snapshot %}

Dimension representing the associated user from salesforce on any day.

The grain of this table is `DIM_CRM_USER_SNAPSHOT_ID` which is a combination of `DIM_CRM_USER_ID` and `DIM_DATE_ID`.

{% enddocs %}

{% docs dim_crm_user_hierarchy_live %}
Dimension table representing the current state of the sales hierarchy, including the user segment, geo, region, and area as it is in the crm user object.

{% enddocs %}

{% docs dim_crm_user_hierarchy_stamped %}
Dimension table representing the sales hierarchy at the time of a closed opportunity, including the user segment. These fields are stamped on the opportunity object on the close date and are used in sales funnel analyses.

{% enddocs %}

{% docs dim_billing_account %}
Dimensional table representing each individual Zuora account with details of person to bill for the account.

The Zuora account creation and maintenance is part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Accounts).

The grain of the table is the Zuora Account.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_hold %}

There are multiple kinds of holds which can be applied to a transaction in the accounting process. This dimension lists the distinct types of holds which may be applied in a revenue contract.

{% enddocs %}

{% docs dim_invoice %}

Dimension table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs dim_location_country %}

Dimensional table for countries mapped to larger regions.

{% enddocs %}

{% docs dim_location_region %}

Dimensional table for geographic regions.

{% enddocs %}

{% docs dim_manual_journal_entry_header %}
High-level details of manual updates made to adjust final totals in accounting reporting.

{% enddocs %}

{% docs dim_manual_journal_entry_line %}

Line-level details of manual updates made to adjust final totals in accounting reporting. This can be mapped directly to a performance obligation in a revenue contract line.

{% enddocs %}

{% docs dim_product_detail %}
Dimensional table representing GitLab's Product Catalog. The Product Catalog is created and maintained through the Price Master Management business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#2-price-master-management).

The Rate Plan Charge that is created on a customer account and subscription inherits its value from the Product Catalog.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan-Charges).

The grain of the table is the Product Rate Plan Charge.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_product_tier %}
Dimensional table representing [GitLab Tiers](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/). Product [delivery type](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/#delivery) and ranking are also captured in this table.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plans).

The grain of the table is the Product Tier Name.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_project %}
Dimensional table representing [GitLab Projects](https://docs.gitlab.com/ee/user/project/). Parent ID (dim_namespace_id) and Ultimate Parent ID (dim_ultimate_parent_id) are also stored in the table

Data comes from [Gitlab Postgres db](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql).

The grain of the table is the Project ID.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_revenue_contract%}

This model contains high-level attributes for all revenue contracts. These can be connected to the corresponding revenue contract lines.

{% enddocs %}

{% docs dim_revenue_contract_hold%}

This model contains attributes for all holds applied to revenue contracts.

{% enddocs %}

{% docs dim_revenue_contract_line%}

This model contains attributes for all revenue contract line items.

{% enddocs %}

{% docs dim_revenue_contract_performance_obligation %}

This model contains attributes for performance obligations that are tied to a revenue contract line.

{% enddocs %}

{% docs dim_revenue_contract_schedule %}

An accounting schedule defines when the company will recognize the revenue of the performance obligation tied to a line in a revenue contract. This model contains the attributes of the schedule that is connected to a give line item.

{% enddocs %}

{% docs dim_subscription %}
Dimension table representing subscription details. The Zuora subscription is created and maintained as part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Subscriptions).

The grain of the table is the version of a Zuora subscription.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_date %}
Dimensional table representing both calendar year and fiscal year date details.

The grain of the table is a calendar day.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_dr_partner_engagement %}
Model to identify the type of business engagement relationship a Partner has with GitLab. The Partner definitions are discussed in the handbook.

[Partner Definitions Handbook Reference](https://about.gitlab.com/handbook/alliances/#partner-definitions)

{% enddocs %}

{% docs fct_campaign %}

Fact table representing marketing campaign details tracked in SFDC.

{% enddocs %}

{% docs fct_crm_account %}
Factual customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as `DIM_CRM_ACCOUNT_ID`.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_crm_attribution_touchpoint %}
Fact table for attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person, dim_crm_opportunity, and dim_crm_account. These touchpoints have revenue associated with them.

{% enddocs %}

{% docs fct_crm_touchpoint %}
Fact table for non-attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person and dim_crm_account.

{% enddocs %}

{% docs fct_crm_opportunity %}

A fact table for salesforce opportunities with keys to connect opportunities to shared dimensions through the attributes of the crm account.

{% enddocs %}

{% docs fct_crm_opportunity_daily_snapshot %}

A daily snapshot fact table for salesforce opportunities with keys to connect opportunities to shared dimensions. This table shows the state of the opportunity as it was on the day of the snapshot.

{% enddocs %}

{% docs fct_crm_person %}

A fact table for Salesforce unconverted leads and contacts. The important stage dates have been included to calculate the velocity of people through the sales funnel. A boolean flag has been created to indicate leads and contacts who have been assigned a Marketo Qualified Lead Date, and a Bizible person id has been included to pull in the marketing channel based on the first touchpoint of a given lead or contact.

{% enddocs %}

{% docs fct_invoice %}

Fact table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs fct_invoice_item %}
Fact table providing invoice line item details.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_charge %}
Factual table with all rate plan charges coming from subscriptions or an amendment to a subscription.

Rate Plan Charges are created as part of the Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Rate-Plan-Charges).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_licenses %}
Dimensional table representing generated licenses and associated metadata.

The grain of the table is a license_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_dotcom_gitlab_emails %}
Dimensional table representing the best email address for GitLab employees from the GitLab.com data source

The grain of the table is a GitLab.com user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}


{% docs dim_gitlab_ops_gitlab_emails %}
Dimensional table representing the best email address for GitLab team members from the Ops.GitLab.Net data source using the gitlab email address to identify GitLab team members

The grain of the table is a Ops.GitLab.Net user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_releases %}
Dimensional table representing released versions of GitLab.

The grain of the table is a major_minor_version.

Additional information can be found on the [GitLab Releases](https://about.gitlab.com/releases/categories/releases/) page.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_versions %}
Dimensional table representing released versions of GitLab.

The grain of the table is a version_id.

Additional information can be found on the [GitLab Releases](https://about.gitlab.com/releases/categories/releases/) page.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_manual_journal_entry_line %}

A fact table of manual journal entry lines which can be connected to a revenue contract line or revenue contract header. These are adjustments made manually as part of the accounting process.

{% enddocs %}

{% docs fct_quote_item %}

A fact table of quote amendments which have quotes and product rate plan charges associated with them. This model connected opportunities to quotes, quote amendments, and products.

{% enddocs %}

{% docs fct_quote %}

Fact table representing quotes pulled from the Zuora billing system. These are associated with crm accounts, billing accounts, opportunities, and subscriptions.

{% enddocs %}

{% docs fct_revenue_contract_hold %}

Details of holds placed on revenue contracts. In the future this will also connect to revenue contract lines that have been placed on hold, but the business does not currently operate this way.

{% enddocs %}

{% docs fct_revenue_contract_line %}
Revenue contract line details including the transaction amount, functional amount, and connections to subscription, performance obligation, crm account, and product details.

{% enddocs %}

{% docs fct_revenue_contract_schedule %}

Schedule showing when revenue will be recognized for all performance obligations connected to a given revenue contract line.

{% enddocs %}

{% docs fct_sales_funnel_partner_alliance_target %}

Sales funnel targets set by the Finance team to measure performance of Partner and Alliances Net ARR, broken down by sales hierarchy, and order attributes.

{% enddocs %}

{% docs fct_sales_funnel_target %}

Sales funnel targets set by the Finance team to measure performance of important KPIs against goals, broken down by sales hierarchy, and order attributes.

{% enddocs %}

{% docs fct_usage_ping_subscription_mapped_smau %}

This model contains **Self-Managed** instances data from every month _that a Usage Ping payload was received_. For a given subscription-uuid-hostname combination, values of each SMAU metric from the last Usage Ping value in that month are reported.

The grain of this table is `hostname` per `uuid` per `dim_subscription_id` per `snapshot_month`. Since there are Self-Managed subscriptions that do not send Usage Ping payloads, it is possible for `uuid` and `hostname` to be null.

This data model is used for the Customer Health Dashboards.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_ci_minutes %}

This table replicates the Gitlab UI logic that generates the CI minutes Usage Quota for both personal namespaces and top level group namespaces. The codebase logic used to build this model can be seen mapped in [this diagram](https://app.lucidchart.com/documents/view/0b8b66e6-8536-4a5d-b992-9e324581187d/0_0).

Namespaces from the `namespace_snapshots_monthly_all` CTE that are not present in the `namespace_statistics_monthly_all` CTE are joined into the logic with NULL `shared_runners_seconds` since these namespaces have not used CI Minutes on GitLab-provided shared runners. Since these CI Minutes are neither trackable nor monetizable, they can be functionally thought of as 0 `shared_runners_minutes_used_overall`. The SQL code has been implemented with this logic as justification.

It also adds two additional columns which aren't calculated in the UI, which are `limit_based_plan` and `status_based_plan` which are independent of whether there aren't projects with `shared_runners_enabled` inside the namespaces and only take into account how many minutes have been used from the monthly quota based in the plan of the namespace.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used to create a mart table (`mart_product_usage_free_user_metrics_monthly`) for Customer Product Insights.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_product_usage_wave_1_3_metrics_latest %}
This table builds on the set of all Zuora subscriptions that are associated with a **Self-Managed** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`prep_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently received Usage Ping and Seat Link per `dim_subscription_id` payload are reported included.

The data from this table will be used to create a mart table (`mart_product_usage_wave_1_3_metrics_latest`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_saas_product_usage_metrics_monthly %}
This table builds on the set of all Zuora subscriptions that are associated with a **SaaS** rate plans. Historical namespace seat charges and billable user data (`gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base`) are combined with high priority Usage Ping metrics (`prep_saas_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently collected namespace "Usage Ping" and membership data per `dim_subscription_id` each month are reported in this table.

The data from this table will be used to create a mart table (`mart_saas_product_usage_monthly`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_event_valid %}

**Description:** Atomic level GitLab.com Usage Event Data with Only Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- Atomic (lowest grain) data  

**Data Grain:**
- event_id
- event_created_at

**Filters:**
- Use ONLY Valid Events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event %}

**Description:** Atomic level GitLab.com Usage Event Data
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- Atomic (lowest grain) data   

**Data Grain:**
- event_id
- event_created_at

**Filters:**
- None - `ALL Data` at the Atomic (`lowest level/grain`) is brought through from the Source for comprehensive analysis.  
  - Futher filters may be needed for Standard Analysis and Reporting, ie. Limiting to Valid Events  

**Business Logic in this Model:**
- The Ultimate Parent Namespace, Plan, Subscription, Billing and Product Information for the Event is determined by the Event Date.
- Each Event is identified as being used for different xMAU metrics (is_smau, is_gmau, is_umau)
- `data_source` = 'GITLAB_DOTCOM'

**Other Comments:**
- The `fct_event` table is built directly from the [Prep_Event table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_event) which brings all of the different types of events together.  A handbook page on this table can be found [here](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-product-events-data/) .
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event_user_daily %}

**Description:** GitLab.com Usage Event Data with Only Valid Events by Event_Date, User, Ultimate_Parent_Namespace and Event_Name
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  

**Data Grain:**
- event_date
- dim_user_id
- dim_ultimate_parent_namespace_id
- event_name

**Filters:**
- Use ONLY Valid Events for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event_instance_daily %}

**Description:** GitLab.com Usage Event Data Grouped by Date and Event for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  
- The data is aggregated by Date and Event and Namespace and includes supporting Attributes.

**Data Grain:**
- event_date
- event_name

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- Event, User and Ultimate_Namespace counts are included for the Aggregation Level

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event_namespace_daily %}

**Description:** GitLab.com Usage Event Data Grouped by Date, Event, Namespace and Billing for Valid Events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.  
- The data is aggregated by Date, Event and Namespace and includes supporting Attributes.

**Data Grain:**
- event_date
- event_name
- dim_ultimate_parent_namespace_id

**Filters:**
- Use Valid Events Only for standard analysis and reporting:
  - Remove Events where the Event Created Datetime < the User Created Datetime.
    - These are usually events from projects that were created before the User and then imported in by the User after the User is created.  
  - Keep Events where User Id = NULL.  These do not point to a particular User, ie. 'milestones'
  - Remove Events from blocked users
- Rolling 24mos of Data  

**Business Logic in this Model:**
- Valid events where the Event Create DateTime is >= User Create DateTime
- Events from blocked users are excluded
- The Actual Ultimate Parent Namespace, Plan, Subscription, Billing and Product Information for the Event is determined by the Event Date.
- Each Event is identified as being used for different xMAU metrics (is_smau, is_gmau, is_umau)
- `data_source` = 'GITLAB_DOTCOM'

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_usage_ping_payload %}
Factual table with metadata on usage ping payloads received.

The grain of the table is a dim_usage_ping_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

Main foreign key that can help to build easily joins:

- dim_license_id
- dim_subscription_id
- dim_date_id

{% enddocs %}

{% docs fct_usage_ping_metric_all_time %}
Factual table on the grain of an individual metric received as part of a usage ping payload.  This model specifically includes only metrics that represent usage over the entire lifetime of the instance.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_self_managed_seat_link %}

Self-managed EE instances send [Seat Link](https://docs.gitlab.com/ee/subscriptions/self_managed/#seat-link) usage data to [CustomerDot](https://gitlab.com/gitlab-org/customers-gitlab-com) on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the [true up process](https://docs.gitlab.com/ee/subscriptions/self_managed/#users-over-license). Counts are reported from the last day of the month for historical months, and the most recent `reported_date` for the current month. Additional details can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_storage %}
This table replicates the Gitlab UI logic that generates the Storage Usage Quotas for top level group namespaces. The logic used to build this model is explained in [this epic](https://gitlab.com/groups/gitlab-org/-/epics/4237). The specific front end logic is described [here](https://gitlab.com/groups/gitlab-org/-/epics/4237#note_400257377).

Storage usage is reported in bytes in source and this is reflected in the `_size` columns. These sizes are then converted into GiB (1 GiB = 2^30 bytes = 1,073,741,824 bytes), and MiB (1 MiB = 2^20 bytes = 1,048,576 bytes), which is most often displayed in the UI. Since storage limits are allocated in GiB, they were left as such in the `_limit` columns.

Since this table reports at the top level namespace grain, aggregation of the individual underlying repositories is required. To increase visibility of the underlying repositories, two count columns (and their associated flags) are added that aren't calculated in the UI: which are `repositories_above_free_limit_count` and `capped_repositories_count`. These columns can serve as helpful indicators for when a customer will likely need to purchase extra storage.

For the purpose of this table, all child namespaces under a top level namespace with unlimited storage are also assumed to have unlimited storage. Also, storage sizes are converted to MiB and GiB in this table because these are the values being reported under the hood, even though on a project page storage is reported as "MB" or "GB".

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_waterfall_summary %}

A derived model using the revenue contract schedule to spread the recognized revenue across from the revenue start date to the revenue end date as defined by the revenue contract performance obligation's schedule.

{% enddocs %}

{% docs dim_usage_pings %}
Dimension that contains demographic data from usage ping data, including additional breaks out for product_tier, if it is from an internal instance, and replaces the ip_address hash with a location_id instead.

[Core represents both CE and EE](https://about.gitlab.com/handbook/marketing/product-marketing/tiers/#history-of-ce-and-ee-distributions).

Get started by exploring the [Product Geolocation Analysis](https://about.gitlab.com/handbook/business-ops/data-team/data-catalog/product-geolocation/) handbook page.
Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_host_instance_type %}

Dimension table providing instance type for a given UUID/HostName pair or Namespace Id for Self-Managed and SaaS data respectively.

{% enddocs %}

{% docs dim_instances %}
Dimension that contains statistical data for instances from usage ping data

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_sales_qualified_source %}

Sales qualified source dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_deal_path %}

Deal path dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_bizible_marketing_channel_path %}

Bizible marketing channel path dimension, based off a grouping of Bizible marketing channel paths in `map_bizible_marketing_channel_path`.

{% enddocs %}

{% docs dim_sales_segment %}

Dimension table for sales segment built off Ultimate_Parent_Sales_Segment_Employees__c in SFDC field in account data. Example values: SMB, Mid-Market, Large

{% enddocs %}

{% docs dim_sales_territory %}

Sales territory dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_industry %}

Industry dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_installation %}

Installation dimension, based off of version usage data and version host table. The primary key is built as a surrogate key based off of the `dim_host_id` and the `dim_instance_id`

{% enddocs %}

{% docs dim_order %}

In Zuora Billing, an [Order](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) represents a complete transaction record. Multiple order actions can be taken in a single order. For example, a subscription can be created and other subscriptions can be managed in a single order for a given customer.

{% enddocs %}

{% docs dim_order_action %}

[Order Actions](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Order_Actions) are the tasks which can be performed on subscriptions in a single Order (see `dim_order` for more details).

The following actions are supported in the Orders module:

- Create Subscription
- Terms And Conditions
- Renewal
- Cancellation
- Owner Transfer
- Add Product
- Update Product
- Remove Product
- Suspend
- Resume

Multiple order actions can be grouped under a single Order. Previously multiple amendments would have been created to accomplish the same result. Now there can be a single `composite` amendment which encompasses all order actions taken in a single order.

{% enddocs %}


{% docs dim_order_type %}

Order type dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_namespace_hist %}

Table containing GitLab namespace snapshots.

The grain of this table is one row per namespace per valid_to/valid_from combination. The Primary Key is `namespace_snapshot_id`.

{% enddocs %}

{% docs dim_namespace_lineage %}

Table containing GitLab namespace lineages. The primary goal of this table is to determine the ultimate parent namespace for all namespaces. Additionally, this table provides plan (GitLab subscription) information for both the given namespace and its ultimate parent namespace.

The grain of this table is one row per namespace. The Primary Key is `dim_namespace_id`.

{% enddocs %}

{% docs dim_namespace_plan_hist %}

Slowly Changing Dimension Type 2 that records changes into namespace's plan subscriptions.

Easily to join with the following tables:

- `dim_namespace` through `dim_namespace_id`

{% enddocs %}

{% docs dim_namespace%}

Includes all columns from the namespaces base model. The plan columns in this table (gitlab_plan_id, gitlab_plan_title, gitlab_plan_is_paid) reference the plan that is inheritted from the namespace's ultimate parent.

This table add a count of members and projects currently associated with the namespace.
Boolean columns: gitlab_plan_is_paid, namespace_is_internal, namespace_is_ultimate_parent

A NULL namespace type defaults to "Individual".
This table joins to common product tier dimension via dim_product_tier_id to get the current product tier.

{% enddocs %}

{% docs dim_order_hist %}

Table containing GitLab order snapshots.

The grain of this table is one row per order per valid_to/valid_from combination.

{% enddocs %}

{% docs dim_quote %}

Dimensional table representing Zuora quotes and associated metadata.

The grain of the table is a quote_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_license %}

Dimensional table representing generated licenses and associated metadata.

The grain of the table is a license_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_key_xmau_metric %}

A fact table that contains only the metrics that is a UMAU, SMAU, or GMAU metric that appears on the [Stages and Groups Performance Indicator handbook page](https://about.gitlab.com/handbook/product/stage-and-group-performance-indicators/)

{% enddocs %}

{% docs dim_ci_pipeline %}

A dim table that contains all CI Pipelines run on Gitlab.com application.

Easy joins available with:

* dim_project through `dim_project_id`
* dim_namespace through `dim_namespace_id` and `ultimate_parent_namespace_id`
* dim_date through `ci_pipeline_creation_dim_date_id`
{% enddocs %}

{% docs dim_action %}

Dimensional table representing actions recorded by the Events API. [More info about actions tracked here](https://docs.gitlab.com/ee/api/events.html)

The grain of the table is the `dim_action_id`. This table is easily joinable with:

- `dim_plan` through `dim_plan_id`
- `dim_user` through `dim_user_id`
- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_namespace_id`

{% enddocs %}

{% docs dim_issue %}

Dimensional table recording all issues created in our Gitlab.com SaaS instance. This table is easily joinable with other EDM dim tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id`
- `dim_plan` through `dim_plan_id`
- `dim_date` through `created_date_dim_id`

More info about issues in GitLab product [available here](https://docs.gitlab.com/ee/user/project/issues/)

{% enddocs %}

{% docs dim_merge_request %}

Dimensional table recording all merge requests created in our Gitlab.com SaaS instance. This table is easily joinable with other EDM dim tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id`
- `dim_plan` through `dim_plan_id`
- `dim_date` through `created_date_dim_id`

More info about issues in GitLab product [available here](https://docs.gitlab.com/ee/user/project/merge_requests/)

{% enddocs %}

{% docs dim_ci_build %}

Dimension table that contains all CI build data.

Easy to join with the following tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_parent_namespace_id`
- `dim_date` through `ci_build_creation_dim_date_id`
- `dim_plan` through `dim_plan_id`

{% enddocs %}

{% docs dim_user %}

Dimension table that contains all Gitlab.com Users.

Missing Column Values:
* Unknown - Value is Null in source data
* Not Found - Row Not found in source data
The following Columns have a Varchar Data Type and are set up to handle Missing Column Values:
* role
* last_activity_date             
* last_sign_in_date               
* setup_for_company    
* jobs_to_be_done
* for_business_use                 
* employee_count
* country
* state

{% enddocs %}

{% docs dim_user_snapshot_bottom_up %}

Snapshot table with Spined Dates that contains all Gitlab.com Users.

Missing Column Values:
* Unknown - Value is Null in source data
* Not Found - Row Not found in source data

The following Columns have a Varchar Data Type and are set up to handle Missing Column Values:
* role
* last_activity_date             
* last_sign_in_date               
* setup_for_company    
* jobs_to_be_done
* for_business_use                 
* employee_count
* country
* state

**Business Logic in this Model:**
- `spined_date` - every date between dbt_valid_from and dbt_valid_to timeframes.  
  - spined_date has a time of '00:00:00' which is less than a dbt_valid_from date with a time greater than 00:00:00, ie 2022-11-14 09:01:37.494.  In this case the spined_date for this snapshot record will be 2022-11-15.  


{% enddocs %}

{% docs dim_ci_runner %}

A Dimension table that contains all data related to CI runners.

It includes keys to join to the below tables:

- `dim_ci_build` through `dim_ci_build_id`
- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_parent_namespace_id`
- `dim_date` through `created_at`
- `dim_date` through `created_date_id `

{% enddocs %}

{% docs dim_ci_stage %}

A dim table that contains all CI Stages run in Gitlab.com CI Pipelines.

Easy joins available with:

* dim_project through `dim_project_id`
* dim_ci_pipeline through `dim_ci_pipeline_id`
* dim_date through `created_date_id`
{% enddocs %}

{% docs fct_ci_runner_activity %}

Fact table containing quantitative data related to CI runner activity on GitLab.com.

{% enddocs %}

{% docs dim_epic %}

Dimensional table representing epics created by groups on Gitlab.com instance. [More info about epics here](https://docs.gitlab.com/ee/user/group/epics/)

The grain of the table is the `dim_event_id`. This table is easily joinable with:

- `dim_plan` through `dim_plan_id`
- `dim_user` through `author_id`
- `dim_namespace` through `group_id` and `ultimate_namespace_id`

{% enddocs %}

{% docs dim_note %}

Dimensional table representing events recorded by the Events API. [More info about events tracked here](https://docs.gitlab.com/ee/api/notes.html)

2 kinds of notes are recorded in the notes table:
- system notes
- users' notes

System notes are notes automatically created based on status changes of the issue/snippet/merge request/epic.

For example, when a user is tagged as a reviewer, a system note is automatically created in the notes table. They are easily identifiable through the `is_system_id` boolean flag.

The grain of the table is the `dim_note_id`. This table is easily joinable with:

- `dim_plan` through `dim_plan_id`
- `dim_user` through `dim_user_id`
- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_namespace_id`
{% enddocs %}

{% docs fct_daily_event_400 %}

Factual table built on top of prep_events tables that allows to explore usage data of free and paid users and namespaces from our SaaS instance gitlab.com.

The granularity is one event per day per user per ultimate parent namespace.

That means if a user creates the same day an issue on the Gitlab Data Team project and 2 issues in the main gitlab-com project, 2 rows will be recorded in the table.

If 2 users A and B create on the same day 1 merge request on the GitLab Data Team projectm 2 rows will be also recorded in the table.

Some examples of analysis that were done with the legacy table `gitlab_dotcom_daily_usage_data_events`:

1. [User Journey Analysis](https://app.periscopedata.com/app/gitlab/869174/WIP-Cross-Stage-Adoption-Dashboard): See how often different product stages are used by the same namespaces. See what stages are used in combination.
1. [New Namespace Stage Adoption](https://app.periscopedata.com/app/gitlab/761347/Group-Namespace-Conversion-Metrics): Evaluate how often new namespaces are adopting stages such as 'Create' and 'Verify' within their first days of use.
1. [Stages per Organization](https://app.periscopedata.com/app/gitlab/824044/Stages-per-Organization-Deep-Dive---SpO): Identify how namespaces adopt stages within their first days and how this correlates with paid conversion and long-term engagement.

{% enddocs %}

{% docs dim_issue_links %}

Dimensional table representing links between GitLab Issues recorded by the Events API. [More info about issue links can be found here](https://docs.gitlab.com/ee/user/project/issues/related_issues.html)

Issue Links are created when relationships are defined between issues. This table has slowly changing dimensions, as issue links/relationships can be removed over time

The grain of the table is the `dim_issue_link_id`. This table is easily joinable with:

- `dim_issue` through `dim_issue_id` on `dim_source_issue_id` & `dim_target_issue_id`
{% enddocs %}

{% docs dim_locality %}

Dimensional table representing the [location_factor](https://about.gitlab.com/handbook/total-rewards/compensation/compensation-calculator/#location-factor) for a given locality and a given time range.

This table is derived from data files and logic of the [compensation calculator](https://gitlab.com/gitlab-com/people-group/peopleops-eng/compensation-calculator), specifically the location_factors.yml and the geo_zones.yml

The grain of the table is the `dim_locality_id` and the `valid_from` date filed.

{% enddocs %}

{% docs dim_usage_ping_metric %}

This model maps directly to the [Gitlab Metrics Dictionary](https://metrics.gitlab.com/). In addition to all metrics currently in the Service Ping, it also contains metrics that have been removed.

{% enddocs %}

{% docs dim_ping_instance %}

**Description:** Atomic Level Instance Service Ping data including Implementation Info along with JSON payloads with Usage Metrics
- Atomic (lowest grain) data with a Single Record per Ping.

**Data Grain:**
- dim_ping_instance_id

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- `ping_delivery_type` = 'SaaS' WHERE UUID/Instance_id = ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f ELSE 'Self-Managed'
- `is_internal` = TRUE WHERE:
  - UUID/Instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
  - (OR) installation_type = 'gitlab-development-kit'
  - (OR) hostname = 'gitlab.com'
  - (OR) hostname LIKE '%.gitlab.com'
- `is_staging` = TRUE WHERE:
  - hostname LIKE 'staging.%'
  - (OR) hostname IN ('staging.gitlab.com','dr.gitlab.com')
- `major_minor_version` = major_version || '.' || minor_version
- `major_minor_version_id` = major_version * 100 + minor_version
- `version_is_prerelease` = version LIKE '%-pre'
- `cleaned_edition` = 'EE Free' WHERE license_expires_at < ping_created_at ELSE ping_edition

**Other Comments:**
- The `dim_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs dim_ping_metric %}

This model replaces `dim_usage_ping_metric` table that maps directly to the [Gitlab Metrics Dictionary](https://metrics.gitlab.com/). In addition to all metrics currently in the Service Ping, it also contains metrics that have been removed.

Some other enhancements in this model include : addition of a surrogate key, exclusion and cleaning of some Product groups, and renaming Usage ping to Service Ping.

{% enddocs %}

{% docs dim_ping_metric_daily_snapshot %}

This slowly changing dimension type 2 model allows for historic reporting of the  `dim_usage_ping_metric` table that maps directly to the [Gitlab Metrics Dictionary](https://metrics.gitlab.com/). `snapshot_id` has been included to be used in the join.

For this reason `metrics_path` is not unique.

{% enddocs %}

{% docs fct_performance_indicator_targets %}

New fact table to replace `performance_indicators_yaml_historical`.

This new table will include all flattened target values for each metric for each month. Can just filter this fact table down in `td_xmau 2.0` snippet.

{% enddocs %}

{% docs fct_ping_instance_metric %}

**Description:** Atomic Level Instance Service Ping Metric data For All Metrics by Ping
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_rolling_13_months %}

**Note** This model is set to never full refresh.  This means that new data will update daily but changes to past events will not be updated.

**Description:** Atomic Level Instance Service Ping Metric data For All Metrics by Ping
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source
- Filters to a rolling 13 months of service pings by the ping_created_date

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_rolling_6_months %}

**Description:** Atomic Level Instance Service Ping Metric data For All Metrics by Ping
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source
- Filters to a rolling 6 months of service pings by the ping_created_date

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance %}

**Description:** Atomic Level Instance Service Ping with Identifiers by Ping
- Atomic (lowest grain) information with basic Identifiers for easy joins out to Dimension Tables.  Includes Instance, Date, Product and Subscriptions id's. Metric detail is not included in the table.
- The data includes a single row per Ping.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.

**Other Comments:**
- The `fct_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_7_day %}

**Description:** Atomic Level Instance Service Ping Metric data For 7 Day Metrics by Ping and Metric
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- time_frame = '7d'
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_28_day %}

**Description:** Atomic Level Instance Service Ping Metric data For 28 Day Metrics by Ping and Metric
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- time_frame = '28d'
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_monthly %}

**Description:** Atomic Level Instance Service Ping data with Last Pings of the Month per Installation by Ping and Metric for 28_Day and All_Time metrics  
- The basic Id's from the source for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source
- Includes metrics for 28 Day and All-Time timeframes
- Include only the `Last Pings of the Month`
- Include `has_timed_out = FALSE` - to remove Metrics that have timed out during ping generation
- Include `metric_value IS NOT NULL` - Only include Metrics with valid Values

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- `is_last_ping_of_month` = last ping created per calendar month per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_weekly %}

**Description:** Atomic Level Instance Service Ping data with Last Pings of the Week per Installation by Ping and Metric for 7_Day metrics  
- The basic Id's from the source for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source
- Includes metrics for 7 Day
- Include only the `Last Pings of the Week`
- Include `has_timed_out = FALSE` - to remove Metrics that have timed out during ping generation
- Include `metric_value IS NOT NULL` - Only include Metrics with valid Values

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- `is_last_ping_of_week` = last ping created per calendar week per Installation (`dim_installation_id`)
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_all_time %}

**Description:** Atomic Level Instance Service Ping Metric data For All-Time Metrics by Ping and Metric
- The data includes a single row per Ping and Metric.  The basic Id's for Subscription, Product and Location are included.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters:**
- time_frame = 'all'
- UUID is NOT NULL  -  Filter In-Valid Data from Source
- version NOT LIKE '%VERSION%'  - Filter In-Valid Data from Source

**Business Logic in this Model:**
- `data_source` = 'VERSION_DB'
  - GitLab, GitLab Dedicated and Self-Managed Service Pings come through the Version Database.
- Metrics that timed out (return -1) are set to a value of 0

**Other Comments:**
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.  
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}


{% docs fct_ping_instance_metric_wave %}

The purpose of this data model is to identify the service pings that can be mapped to a subscription and to pivot thet set of [wave metrics required for Gainsight](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/ping_instance_wave_metrics.sql) from the `fct_ping_instance_metric` model, strip all the sensitive data out, and then report one value for each metric in that column.

{% enddocs %}


{% docs fct_ping_instance_metric_wave_monthly %}

This table builds on the set of all Zuora subscriptions that are associated with a **Self-Managed** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`fct_ping_instance_metric_wave`) to build out the set of facts included in this table.

The grain of this table is `hostname` per `dim_instance_id(uuid)` per `dim_subscription_id` per `snapshot_month`. Since there are Self-Managed subscriptions that do not send Usage Ping payloads, it is possible for `dim_instance_id` and `hostname` to be null.

The data from this table will be used to create a mart tables -  `mart_product_usage_paid_user_metrics_monthly` and `mart_product_usage_wave_1_3_metrics_monthly_diff` for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_mrr_snapshot_model %}

Daily [snapshot](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#snapshots) model of the [fct_mrr](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.fct_mrr) model

{% enddocs %}

{% docs dim_subscription_snapshot_model %}

Daily [snapshot](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#snapshots) model of the [dim_subscription](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.dim_subscription) model

{% enddocs %}
