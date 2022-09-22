### YML Extractor

This job pulls an Airflow-generated JSON files into tables in Snowflake within the raw.gitlab_data_yaml schema. It uses [remarshal](https://pypi.org/project/remarshal/) to serialize `YAML` to `JSON`. This is done in the `gitlab_data_yaml.py` DAG in the `dags/extract` directory.

Current files are:

* [Location Factors](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/location_factors.yml)
* [Roles](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/roles.yml)
* [Team](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/team.yml)


### API Access

We are using a `project level token` created in the `compensation calculator` project to access the API.

In order to create one if the existing token has expired, you need to login as the `Analytics API` user into GitLab.com and create a new token for the `compensation calculator` project [here](https://gitlab.com/gitlab-com/people-group/peopleops-eng/compensation-calculator/-/settings/access_tokens). 

The token is stored in our vault as part of the `Analytics API GitLab Login`.

The environment variable called `GITLAB_API_PRIVATE_TOKEN` needed by the upload script is stored as part of the `airflow` k8s secret and needs to be updated accordingly.
