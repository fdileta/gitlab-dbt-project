### Scheduling Notebook Request

#### Requestor To Complete

1. [ ] If creating a new Data Science model ensure that all necesaary steps have been completed from the [Scoring Template](https://gitlab.com/gitlab-data/data-science/-/blob/main/templates/propensity/Score%20Model.ipynb)
   - To read data from Snowflake, _**Step 2**_ must be completed
   - To write data back to a Snowflake table, _**Step 10**_ must be completed
1. [ ] (Optional) If notebook outputs will be imported into Snowflake, specify table name: ` `
1. [ ] Create a deployment or prod directory in your project repository from which the notebook will run. 
   - This should contain all of the snowflake queries, parameter (.yml) files, model file, notebook(s), and any additional files needed to run the process.
   - For example, see the [PtE Prod directory](https://gitlab.com/gitlab-data/data-science-projects/propensity-to-expand/-/tree/main/prod) 
1. [ ] Specify process name for Airflow: ` `
1. [ ] Specify command Papermill will use to execute the code: ` `
   - For example, in the PTE Dag a parameter was created to specify the environment: `papermill scoring_code.ipynb -p `**`is_local_development False`**
   - Unless you are need multiple notebooks to execute, the above command should suffice.
1. [ ] Specify requested schedule: ` `
   1. The schedule should be specified using cron tab scheduling.
   2. For examples and help specifying a schedule, see [CronTab Guru](https://crontab.guru/#*_*_*/1__)
1. Specify table names and structure for the scores source table and the final view.
    1. Usually the source table is named as follows: `[model_name]_scores` (ex. `ptc_scores`, `ptpt_scores`, `namespace_segmentation_scores`)
    1. Existing examples (the raw table is the one where the JSON scores are loaded directly from the notebook and the prod view is how these values are exposed to the `workspace_data_science` schema): `raw.data_science.ptc_scores (JSONTEXT VARIANT, UPLOADED_AT TIMESTAMP_NTZ(9))` and `prod.workspace_data_science.ptc_scores(CRM_ACCOUNT_ID VARCHAR, SCORE_DATE TIMESTAMP, SCORE NUMBER(38,4), DECILE NUMBER(38,0), IMPORTANCE NUMBER(38,0), SCORE_GROUP NUMBER(38,0), INSIGHTS VARCHAR, RENEWAL_DATE TIMESTAMP, UPLOADED_AT TIMESTAMP)`
1. [ ] Ping @data-team/engineers and request the below process


#### Data Engineer To Complete

1. [ ] Create MR for new DAG in analytics repo under [/dags/data_science](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science) folder.(An example of a previous similar MR can be found [here](https://gitlab.com/gitlab-data/analytics/-/merge_requests/6725).)
   1. The [propensity_to_expand](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science/ds_propensity_to_expand.py) DAG can be used as a template.
   2. Only updates / changes should be for parameters specified above.
      - Ensure the below fields are updated
      1. [ ] Name
      2. [ ] Schedule
      3. [ ] Path
      4. [ ] Parameters
   3. Create the source table as specified in the definition given by the requestor; make sure to create it using the `LOADER` role
   4. Create the corresponding dbt model unpacking the fields from the json in the raw table
   4. When a new model is being productionalized, a `stage` has to be created on Snowflake (database RAW, schema `DATA_SCIENCE`)
      1. [ ] Make sure you are creating the stage using the `LOADER` role
      2. [ ] Stage name should be following this name convention: `{model_short_name}_LOAD` (Examples of existing stages: `PTE_LOAD`, `PTC_LOAD`, `PTPT_LOAD`)
      3. [ ] Make sure to grant all privileges on the newly created stage to the `DATA_SCIENCE_LOADER` & `TRANSFORMER` role (permissions on stages are not handled via Periscope, which is how we generally handle Snowflake permissions)
      4. [ ] Make sure to update `roles.yml` (/permissions/snowflake/roles.yml) to add all the necessary permissions (if any new objects are queried by the new notebook) to the `DATA_SCIENCE_LOADER` role
      5. Make sure to add the new model to the function `data_science_pipelines_dag` in [here](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/airflow_utils.py), so that Airflow errors related to the newly introduced model will be sent to the #data-science-pipelines Slack channel
