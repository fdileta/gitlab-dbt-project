### Goal
Extract data from the Clari API, specifically for the `net_arr` forecast report.


### API process
To get the 'net_arr' report data, 3 endpoints need to be called, [Clari Docs](https://developer.clari.com/default/documentation/external_spec):
1. export endpoint: start the `net_arr` export
    - There is an alternative endpoint `/forecast/{forecastId}`, but this only returns the latest week of each quarter.
2. job status endpoint: poll until the job is 'DONE'
3. results endpoint: returns the report as a JSON object

The final JSON object is uploaded to Snowflake.

## DAG

### Backfills
Backfills are strongly discouraged/prohibited. 

The Clari API forecast endpoint is NOT idempotent, that is, there is no guarantee that calling the endpoint with the same parameters will generate the same result.

Support has confirmed the following: 
*Clari historical export does not provide historical entries from users who are not currently in the forecast opted-ins.*

That means regardless of the quarter, a forecast is generated only, and for only **currently active** employees. Once an employee becomes inactive, it is no longer possible to retrieve their previously forecasted values from the API.

Since backfills are discouraged, there is no mechanism provided to perform them.

### DAG Scheduling

#### Daily DAG
The Daily DAG is scheduled to run daily at 8:00am UTC to ensure that the latest updates have been captured, but before the dbt-run has started. 

This is because entries can be updated up to 8:00am UTC the next day after their 'week_end_date', here's an example:

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

The Daily DAG will use the `{{ execution_date }}` for the fiscal quarter, this will correspond to **yesterday's** fiscal quarter.

Furthermore the daily DAG uses `isHistorical=False`

#### Quarterly DAG
The Quarterly DAG is scheduled to run at the beginning of the quarter. It will use the ` {{ next_execution_date }} `, which means the fiscal_quarter will correspond to the 1st day of quarter when it's actually run.

The purpose of this DAG is to gather all the outstanding data for the new quarter using a 'isHistorical=True` when calling the API.

Why is there outstanding data if it's only the 1st of the quarter? The 'week_start_date' associated with that quarter may unintuitively start earlier.

As an example, 'Week 1' of Q1 might begin on Jan 25, rather than the expected Feb 1st, and you may lose that first week if 'isHistorical=False'.

Originally, the quarterly DAG was also created for backfills, all that needs to be done is to set the `start_date` of the DAG to when the user wants to start backfilling, but as discussed above, backfills should not be done.

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


# To delete possibly (all the below)

## Design Considerations

### IsHistorical = True
Currently, the API parameter `isHistorical=True` is being used. If 'False', it only includes the entries from the current week. 

This creates a lot of duplicate data, because each request, the entire quarter's worth of data is being returned in the payload.

The reason the parameter is set to 'True' is because it makes it easier to guarantee that no weeks are being excluded.

This is because it's hard to predict exactly when the first 'week' of the quarter starts. 

The first (and sometimes second week) starts before the actual quarter start-date, for example 'Week 1' of Q1 might begin on Jan 25, rather than the expected Feb 1st, and you may lose that first week if the flag is False.


### Transformation logic - prefiltering the JSON

# TODO (update depending on the end design decision)

