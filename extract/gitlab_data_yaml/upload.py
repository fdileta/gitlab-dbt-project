"""Source code to perform extraction of YAML file from Gitlab handbook, internal handbook, comp calculator"""
import logging
import subprocess
import sys
import requests
import base64
import json
import yaml

from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    handbook_dict = dict(
        categories="categories", stages="stages", release_managers="release_managers"
    )

    pi_file_dict = dict(
        chief_of_staff_team_pi="chief_of_staff_team",
        customer_support_pi="customer_support_department",
        development_department_pi="development_department",
        engineering_function_pi="engineering_function",
        finance_team_pi="finance_team",
        infrastructure_department_pi="infrastructure_department",
        marketing_pi="marketing",
        people_success_pi="people_success",
        quality_department_pi="quality_department",
        security_department_pi="security_department",
        ux_department_pi="ux_department",
    )

    pi_internal_hb_file_dict = dict(
        #corporate_finance_pi="corporate_finance",
        dev_section_pi="dev_section",
        enablement_section_pi="enablement_section",
        ops_section_pi="ops_section",
        product_pi="product",
        recruiting_pi="recruiting",
        sales_pi="sales",
        secure_and_protect_section_pi="secure_and_protect_section",
    )

    comp_calc_dict = dict(
        location_factors="location_factors", roles="job_families", geo_zones="geo_zones"
    )

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    gitlab_in_hb_token = env.get("GITLAB_INTERNAL_HANDBOOK_TOKEN")

    HANDBOOK_URL = "https://gitlab.com/gitlab-com/www-gitlab-com/raw/master/data/"
    pi_url = f"{HANDBOOK_URL}performance_indicators/"

    # Internal handbook url
    pi_internal_hb_url = "https://gitlab.com/api/v4/projects/26282493/repository/files/data%2Fperformance_indicators%2F"

    comp_calc_url = (
        "https://gitlab.com/api/v4/projects/21924975/repository/files/data%2F"
    )

    TEAM_URL = "https://about.gitlab.com/company/team/"
    usage_ping_metrics_url = "https://gitlab.com/api/v4/usage_data/metric_definitions/"

    job_failed = False

    def request_download_decode_upload(
        table_name, file_name, base_url, private_token=None, suffix_url=None
    ):
        """This function is designed to stream the API content by using Python request library.
        Also it will be responsible for decoding and generating json file output and upload
        it to external stage of snowflake. Once the file gets loaded it will be deleted from external stage.
        This function can be extended but for now this used for the decoding the encoded content"""
        logging.info(f"Downloading {file_name} to {file_name}.json file.")

        # Check if there is private token issued for the URL
        if private_token is not None:
            request_url = f"{base_url}{file_name}{suffix_url}"
            response = requests.request("GET", request_url, headers={"Private-Token": private_token})

        # Load the content in json
        api_response_json = response.json()
        #check if the file is empty or not present.
        record_count = len(api_response_json)
        if record_count > 1:
            # Get the content from response
            file_content = api_response_json.get("content")
            message_bytes = base64.b64decode(file_content)
            output_json_request = yaml.load(message_bytes, Loader=yaml.Loader)
            # write to the Json file
            with open(f"{file_name}.json", "w") as f:
                json.dump(output_json_request, f, indent=4)
            logging.info(f"Uploading to {file_name}.json to Snowflake stage.")

            snowflake_stage_load_copy_remove(
            f"{file_name}.json",
            "gitlab_data_yaml.gitlab_data_yaml_load",
            f"gitlab_data_yaml.{table_name}",
            snowflake_engine,
        )
        else:
            logging.error(f"The file for {file_name} is either empty or the location has changed investigate")
            job_failed = True

    def curl_and_upload(table_name, file_name, base_url, private_token=None):
        """This function uses Curl to download the file and convert the YAML to JSON.
        Then upload the JSON file to external stage and then load it snowflake.
        Post load the files are removed from the external stage"""
        if file_name == "":
            json_file_name = "ymltemp"
        elif ".yml" in file_name:
            json_file_name = file_name.split(".yml")[0]
        else:
            json_file_name = file_name

        logging.info(f"Downloading {file_name} to {json_file_name}.json file.")

        if private_token is not None:
            header = f'--header "PRIVATE-TOKEN: {private_token}"'
            command = f"curl {header} '{base_url}{file_name}%2Eyml/raw?ref=main' | yaml2json -o {json_file_name}.json"
        else:
            command = f"curl {base_url}{file_name} | yaml2json -o {json_file_name}.json"

        try:
            p = subprocess.run(command, shell=True)
            p.check_returncode()
        except:
            job_failed = True

        logging.info(f"Uploading to {json_file_name}.json to Snowflake stage.")

        snowflake_stage_load_copy_remove(
            f"{json_file_name}.json",
            "gitlab_data_yaml.gitlab_data_yaml_load",
            f"gitlab_data_yaml.{table_name}",
            snowflake_engine,
        )

    for key, value in handbook_dict.items():
        curl_and_upload(key, value + ".yml", HANDBOOK_URL)

    for key, value in pi_file_dict.items():
        curl_and_upload(key, value + ".yml", pi_url)

    # Iterate over Internal handbook
    for key, value in pi_internal_hb_file_dict.items():
        request_download_decode_upload(
            key, value, pi_internal_hb_url, gitlab_in_hb_token, f"%2Eyml?ref=main"
        )

    for key, value in comp_calc_dict.items():
        curl_and_upload(
            key,
            value,
            comp_calc_url,
            config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"],
        )

    curl_and_upload("team", "team.yml", TEAM_URL)
    curl_and_upload("usage_ping_metrics", "", usage_ping_metrics_url)

    if job_failed:
        logging.error(f"Search for value 'ERROR:root:The file for'in the log file for internal handbook extraction.")
        sys.exit(1)
