/*
 * This is one time SQL script in order to align tables and update missing data.
 * After review, will be deleted.
 * After review schema name will be replaced:
 * "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW" -> "RAW"
 */

-- Crete temp table
CREATE TABLE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS_OLD"
CLONE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
COPY GRANTS;

-- Drop original table (need to replace data type VARIANT -> VARCHAR(16777216) and "ALTER TABLE ... MODIFY COLUMN ..." doesn't work for
-- this case.
DROP TABLE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS";

-- Create original table (with chaned VARIANT to VARCHAR)
CREATE TABLE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
(jsontext     VARCHAR(16777216),
 ping_date    DATE,
 run_id       VARCHAR(40),
 _uploaded_at FLOAT);

-- Put data back into original table
INSERT INTO "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
(jsontext,ping_date,run_id,_uploaded_at)
SELECT jsontext AS jsontext,
       NULL     AS ping_date,
       NULL     AS run_id,
       NULL     AS _uploaded_at
  FROM "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS_OLD";

-- Back fill for missing values
UPDATE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
   SET run_id = MD5_HEX(RANDOM())
 WHERE run_id IS NULL;

UPDATE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
   SET ping_date = TRIM(SUBSTR(try_parse_json(jsontext):"recording_ce_finished_at",1,10))::DATE
 WHERE ping_date IS NULL;

UPDATE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
   SET _uploaded_at = DATE_PART(EPOCH_SECOND, TRIM(REPLACE(SUBSTR(try_parse_json(jsontext):"recording_ce_finished_at",1,19),'T',' '))::TIMESTAMP)
 WHERE _uploaded_at IS NULL;


COMMIT;

/***********************************************************************************************************************
 * Test cases
 ***********************************************************************************************************************/
-- Test case 1/3 after updating and loading from local Airflow.
SELECT COUNT(1)
  FROM "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS"
 WHERE ping_date    IS NULL
    OR jsontext     IS NULL
    OR run_id       IS NULL
    OR _uploaded_at IS NULL;

-- no rows returned, as expected

-- Test case 2/3 for instance_redis_metrics DBT job
SELECT COUNT(1)
  FROM RBACOVIC_PREP.SAAS_USAGE_PING.INSTANCE_REDIS_METRICS
 WHERE ping_date    IS NULL
    OR response     IS NULL
    OR run_id       IS NULL
    OR _uploaded_at IS NULL;

-- no rows returned, as expected


-- Test case 3/3 for WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS DBT job
SELECT ping_date,
       metric_path,
       COUNT(1)
  FROM RBACOVIC_PROD.LEGACY.WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS
 -- WHERE to_char(recorded_at,'yyyy-mm-dd') = '2021-09-13'
 GROUP BY ping_date,
          metric_path
 HAVING COUNT(1) > 1;

-- no rows returned, as expected


-- If everything is OK, drop temp table
DROP TABLE "10272-DUPLICATE-ENTRIES-IN-PROD-LEGACY-WK_SAAS_USAGE_PING_INSTANCE_REDIS_METRICS_RAW"."SAAS_USAGE_PING"."INSTANCE_REDIS_METRICS_OLD";