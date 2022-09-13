## Find ready to process requests
Snowflake Deletion is the last step in the [GDPR deletion process](https://gitlab.com/gitlab-com/gdpr-request/-/blob/master/.gitlab/issue_templates/deletion_meta_issue.md) and so first we identify ready requests using [this search](https://gitlab.com/gitlab-com/gdpr-request/-/issues?scope=all&utf8=%E2%9C%93&state=opened&label_name[]=data-removal&not[label_name][]=GitLab-removal)

## Run data-removal script

For each request:
1. [ ] Mark the request issue as related to this issue
1. [ ] Determine if it applies to GitLab.com data source only or all data sources
   1. [ ] For GitLab.com data source only, follow the hashing and deletion process documented [in the dbt gdpr deletion gitlab_dotcom macro](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.gdpr_delete_gitlab_dotcom)
   1. [ ] For all data sources, follow the hashing and deletion process documented [in the dbt gdpr deletion macro](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.gdpr_delete)
1. [ ] Comment in the request that the removal has been processed with the attach results file
1. [ ] Unassign data team members and remove the `~data-removal` tag
1. [ ] Check the box for snowflake in the removal request issue description


/label ~"Team::Data Platform" ~Legal ~Housekeeping ~Snowflake ~"Priority::1-Ops" ~"workflow::4 - scheduled" 
