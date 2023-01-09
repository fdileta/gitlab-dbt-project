{% docs rpt_delta_arr_parent_month_8th_calendar_day %}

This report provides the 8th calendar day snapshot for the mart_delta_arr_parent_month table. It uses the rpt_delta_arr_parent_month_8th_calendar_day to build the table.

Custom Business Logic:

1. The parent/child crm account hierarchy changes on a monthly basis. The ARR snapshot table captures the account hierarchy on the 8th calendar day. When doing month over month calcs, this can result in accounts showing as churn in the snapshot data, but in reality, they just changed account hierarchies and did not churn. Therefore, we use the live crm account hierarchy in this model to remove the error that results from looking at snapshot account hierarchies.

2. We started snapshotting the product ranking in August 2022. Therefore, we have to use the live product ranking to backfill the data. In the future, this can be refined to use a dim_product_detail snapshot table when it is built.

{% enddocs %}

{% docs rpt_delta_arr_parent_product_month_8th_calendar_day %}

This report provides the 8th calendar day snapshot for the mart_delta_arr_parent_product_month table. It uses the rpt_delta_arr_parent_month_8th_calendar_day to build the table.

Custom Business Logic:

1. The parent/child crm account hierarchy changes on a monthly basis. The ARR snapshot table captures the account hierarchy on the 8th calendar day. When doing month over month calcs, this can result in accounts showing as churn in the snapshot data, but in reality, they just changed account hierarchies and did not churn. Therefore, we use the live crm account hierarchy in this model to remove the error that results from looking at snapshot account hierarchies.

2. We started snapshotting the product ranking in August 2022. Therefore, we have to use the live product ranking to backfill the data. In the future, this can be refined to use a dim_product_detail snapshot table when it is built.

{% enddocs %}
