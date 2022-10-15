<!-- Subject format should be: YYYY-MM-DD | task name | Error line from log-->
<!-- example: 2020-05-15 | dbt-non-product-models-run | Database Error in model sheetload_manual_downgrade_dotcom_tracking -->

Notification Link: <!-- link to airflow log with error / Monte Carlo incident -->

```
{longer error description text from log}
```

Downstream Airflow tasks or dbt models that were skipped: <!-- None -->
  <!-- list any downstream tasks that were skipped because of this error -->

## AE Triage Guidelines

<details>
<summary><b>dbt model/test failures</b></summary>

As we work to incorporate Monte Carlo into the AE workflow, it will be a bit nuanced and choppy as we make the transition. To help smooth out the process, the triager should triage the logs in this order: DBT Run first, DBT Test second, and Monte Carlo third. The target state would be for us to not triage DBT test once we move completly over to Monte Carlo for testing triage, but in the transition period, we will have to triage all 3 logs.

**DBT Run Triage:**

1. [ ] Check the dbt audit columns in the model to see who created the model, who last updated the model, and when.
1. [ ] If the model was created within the last month, then assign the test or run failure issue to that developer. This will allow for a 1 month warranty period on the model where the creator of the model can resolve any test or run problems.
1. [ ] For models outside of the 1 month warranty period, check out the latest master branch and run the model locally to ensure the error is still valid. 
1. [ ] For models outside of the 1 month warranty period, check the git log for the problematic model, as well as any parent models. If there are any changes here which are obviously causing the problem, you can either:
    1. [ ] If the problem is syntax and simple to solve (i.e. a missing comma) create an MR attached to the triage issue and correct the problem. Tag the last merger for review on the issue to confirm the change is correct and valid.
    1. [ ] If the problem is complicated or you are uncertain on how to solve it tag the CODEOWNER for the file.

**DBT Test Specific Triage Steps:**

The target state is to move all DBT tests over to Monte Carlo for monitoring and eventually stop running the dbt test job in the Airflow DAG. This will provide for one user interface for triaging. It will take a few quarters for us to achieve this target state. We will still have DBT tests that we set-up and use for local development and CI testing in the Data Tests Project and schema.yml files (uniqueness and not null tests on keys in particular), but the target state is to only triage the test failures via Monte Carlo. We will iterate through this and start by setting up monitors for failing DBT tests in Monte Carlo via the triage process. **In addition to the steps outlined above in the DBT Run Triage section, the below steps should be followed to work through the DBT test failures.**  

1. [ ] Check to see if there is a monitor for the test failure in Monte Carlo. If there is a monitor for it, then you only need to triage the test failure in Monte Carlo and can follow the Monte Carlo protocal for triaging. 
1. [ ] For DBT test failures where there is not a monitor set-up in Monte Carlo, the the triager should set-up a monitor in Monte Carlo for the test. The triager can then proceed to triage that test failure via the Monte Carlo protocal for triaging.
1. [ ] The row count tests and not null tests on columns that are not keys are not very effective in DBT. For these types of tests where Monte Carlo does a better job, we can proceed to move the test over to Monte Carlo AND deprecate it from dbt since the tests are not effective and have historically been set to a warn setting in DBT.

**Monte Carlo Triage:**

Below are some tips, tricks, and methods to evaluate some routine and periodic Monte Carlo test failures:

How to resolve and reconcile the related alerts for a DBT Model Run failure and the resulting volume alert failures in Monte Carlo?

1. [ ] Determine if a DBT Model Run failure is the root cause of the volume alerts in Monte Carlo. If so, the triager can simply reference all of the relevant Monte Carlo alerts and failures in the DBT Model run issue. There is no need to open multiple issues for the same root cause DBT Model Run failure.
1. [ ] Determine if the Monte Carlo Alert or Failure is a result of an extraction. Check the extraction logs or incident issues for more details. **WIP: List more specific detailed steps on the methods to evaluate extraction failures. 

</details>

<details>
<summary><b>Resolving Chronic dbt model/test failures</b></summary>
For chronic dbt model and test failures that have been around for more than 1 month, please complete the below steps:

1. [ ] Has the root cause of the failure been determined? If not, the triager should determine the root cause.
1. [ ] Is the root cause of the failure upstream in a source system? **WIP: Consider identifying the source system owner and getting them alerted to the data quality problem via a montior in Monte Carlo and a Slack channel.**
1. [ ] Is the dbt model/test failure related to a timeout issue? **WIP: Follow the Guidance given in the Data Model Performance Handbook Page to resolve the problem.**
1. [ ] Are there multiple tests failing, across multiple models for the same root cause? Consider identifying the model with the root cause failure, keep the test on that model, and deprecate the remaining tests. This will help clear out noise and redundancy in the test logs.

</details>


/label ~Triage ~Break-Fix ~"Priority::1-Ops" ~"workflow::1 - triage" ~"Triage::Analytics"
