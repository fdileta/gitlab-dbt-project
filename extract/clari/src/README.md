### Goal
Extract data from the Clari API, specifically for the `net_arr` forecast report.


### API process
To get the 'net_arr' report data, 3 endpoints need to be called:
1. export endpoint: start the `net_arr` export
2. job status endpoint: poll until the job is 'DONE'
3. results endpoint: returns the report as a json object

The final json object is uploaded to Snowflake.

### DAG Design
There are two DAG's, a `daily` and `quarterly` DAG.

The 'daily' DAG is needed because the stakeholders want the data refreshed daily.

The 'quarterly' DAG is necessary for two reasons:
1. Late Arriving events: As soon as a new quarter begins, the daily DAG will begin requesting the data for the new *quarter*. However, the previous quarter predictions may still not be finalized, so this quarterly DAG will be an additional and final run after the quarter closes to bring in any updated records.
2. Backfills

#### Alternative DAG design
Instead of two DAG's, the other option would be to have one DAG with the following tasks:
daily_task -> shortcircuit_operator to check if quarter ended -> quarterly_task.

However, such a solution makes it hard to create an `idempotent` DAG. For the pipeline to be idempotent, the fiscal_quarter should be derived off the task {{ execution date }}.

This forecast endpoint does not take in a start/end time. Instead, it takes in a quarter.

For previous quarters, we only need to call the API once for that quarter. 
However, for the current quarter, because the stakeholders want daily updates, the API needs to be called daily.

To accommodate both schedules yet maintain an idempotent solution, two separate schedule_intervals, and thus two separate DAGs need to be created.

### Setup Database Environment
#### Create Stage Command
```sql
use raw.clari;

CREATE STAGE clari_load
FILE_FORMAT = (TYPE = 'JSON');
```

#### Create Table Command
Execute following command for creating new table in RAW database
```sql
CREATE OR REPLACE TABLE raw.clari.net_arr (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```
