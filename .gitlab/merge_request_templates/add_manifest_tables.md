Closes

* List the tables added/changed below
* If tables are non-SCD add them to reconcile process.
* Run the `clone_raw_postgres_pipeline` CI job
* Run the `pgp_test` or `gitlab_ops_pgp_test` CI job by right clicking on the job name and opening in a new tab
  * For `pgp_test`, include the `MANIFEST_NAME` variable and input the name of the db (i.e. `gitlab_com`, `customers`, etc.)
  * If this is a SCD table be sure to include:
    * `advanced_metadata: true` in the manifest
    * `TASK_INSTANCE` variable in job trigger with any value (i.e. `mr-2112`)

#### Tables Changed/Added

* [ ] List

* [ ] If tables are non-SCD add them to reconcile process.

#### PGP Test CI job passed?

* [ ] List

#### Red data

* [ ] Confirm no [red data](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/new-data-source/#red-data) is loaded into Snowflake or the data is masked.
