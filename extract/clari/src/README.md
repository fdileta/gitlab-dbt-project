### Goal
Extract data from the Clari API, specifically for the `net_arr` forecast report.

### One-time Setup of Database Environment
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


### API process
There are two forecast API endpoints, [Clari Docs](https://developer.clari.com/default/documentation/external_spec), and two DAG's that call each endpoint (described in more detail below in 'DAG' section.

Both endpoints return a  final JSON object that is uploaded to Snowflake.

#### Current week forecast
The `/forecast/{forecastId}` endpoint only returns the latest week of each quarter. Called by the 'Daily' DAG.

#### Historical forecast
To get 'historical' data, 3 endpoints need to be called
1. export endpoint: start the `net_arr` export
2. job status endpoint: poll until the job is 'DONE'
3. results endpoint: returns the report as a JSON object

Called by the 'Quarterly' DAG.

## DAG

### Backfills
Backfills are strongly discouraged/prohibited. 

The Clari API forecast endpoint is NOT idempotent, that is, there is no guarantee that calling the endpoint with the same parameters will generate the same result.

Support has confirmed the following: 
*Clari historical export does not provide historical entries from users who are not currently in the forecast opted-ins.*

That means regardless of the quarter, a forecast is generated only, and for only **currently active** employees. Once an employee becomes inactive, it is no longer possible to retrieve their previously forecasted values from the API.

Since backfills are discouraged, there is no mechanism provided to perform them.


### Daily DAG
The Daily DAG has the following attributes:
1. Calls the 'current week forecast' endpoint
1. is scheduled to run daily at 8:00am UTC to ensure that the latest updates have been captured, but before the dbt-run has started. 
1. The Daily DAG will use the `{{ execution_date }}` for the fiscal quarter, this will correspond to **yesterday's** fiscal quarter.

### Quarterly DAG
The Quarterly DAG has the following attributes:
1. Calls the 'historical forecast' endpoint
1. The Quarterly DAG is scheduled to run on the first day of each GitLab quarter, i.e Feb, Apr, Aug, Nov. At 8:05am to ensure it finishes prior to dbt-run job, but starts slightly after the daily run so that it has a later 'uploaded_at'

It will consist of two tasks:
- Previous quarter
    - It will use the ` {{ execution_date }} `, which means the fiscal_quarter will correspond to the previous quarter.
    - This is necessary to refresh any records that were updated. There is a corner-case for a small subset of records where the previous week's data is updated on the first day of the new week.
- New quarter: 
    - It will use the ` {{ next_execution_date }} `, which means the fiscal_quarter will correspond to the 1st day of quarter when it's actually run.
    - This is necessary to capture all weeks- not just the current week- corresponding to the new quarter.
    - As an example, 'Week 1' of Q1 might begin on Jan 25, rather than the expected Feb 1st, and you may lose that first week if 'isHistorical=False'.


Originally, the quarterly DAG was also created for backfills, all that needs to be done is to set the `start_date` of the DAG to when the user wants to start backfilling, but as discussed above, backfills should not be done.

### Updated records
Records can be updated up till 8:00am UTC of the next day after their 'week_end_date', here's an example:

```sql
SELECT
 time_frame_id,
  '2022-12-07' week_start_date,
  '2022-12-13' week_end_date,
  updated_on
FROM
  PREP.CLARI.NET_ARR_ENTRIES
WHERE
  time_frame_id = 'TF:2022-12-07'
  AND user_id = '280627:00E4M000001RgqRUAS'
  AND field_id = 'fc_net_commit';
```

This is accounted for by the daily DAG- scheduled at 8am- except in the corner case where for a small subset of records from the previous week's data is updated on the first day of the new week. 

The quarterly DAG accounts for this edge-case as it runs a historical run one day after the quarter ends.

