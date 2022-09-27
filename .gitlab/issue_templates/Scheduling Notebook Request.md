### Scheduling Notebook Request

#### Requestor To Complete

1. [ ] Specify process name: ``
2. [ ] Create MR to the [data-science/deployments](https://gitlab.com/gitlab-data/data-science/-/tree/main/deployments) folder
   1. The MR should create a new folder containing all of the queries required, along with the Notebook. The [pte folder](https://gitlab.com/gitlab-data/data-science/-/tree/main/deployments/pte) can be used as an example
3. [ ] Specify command line Papermill notebook parameters, if any are needed: ``
   1. For example, in the PTE Dag a parameter was created to specify the environment: `papermill scoring_code.ipynb -p `**`is_local_development False`**
4. [ ] Specify requested schedule: ``
   1. The schedule should be specified using cron tab scheduling.
   2. For examples and help specifying a schedule, see [CronTab Guru](https://crontab.guru/#*_*_*/1__)
5. [ ] Ping @data-team/engineers and request the below process


#### Data Engineer To

1. [ ] Create MR for new DAG in analytics repo under [/dags/data_science](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science) folder.
   1. The [propensity_to_expand](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science/propensity_to_expand.py) DAG can be used as a template.
   2. Only updates / changes should be for parameters specified above.
      - Ensure the below fields are updated
      1. [ ] Name
      2. [ ] Schedule
      3. [ ] Path
      4. [ ] Parameters
   3. When a new model is being productionalized, a `stage` has to be created on Snowflake (database RAW, schema `DATA_SCIENCE`)
      1. [ ] Make sure you are creating the stage using the `LOADER` role
      2. [ ] Stage name should be following this name convention: `{model_short_name}_LOAD` (Examples of existing stages: `PTE_LOAD`, `PTC_LOAD`, `PTPT_LOAD`)
      3. [ ] Make sure to grant all privileges on the newly created stage to the `DATA_SCIENCE_LOADER` & `TRANSFORMER` role (permissions on stages are not handled via Periscope, which is how we generally handle Snowflake permissions)
      4. [ ] Make sure to update `roles.yml` (/permissions/snowflake/roles.yml) to add all the necessary permissions (if any new objects are queried by the new notebook) to the `DATA_SCIENCE_LOADER` role