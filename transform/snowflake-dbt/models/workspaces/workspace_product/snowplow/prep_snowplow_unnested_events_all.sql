{{config({
    "materialized":"view"
  })
}}

-- depends_on: {{ ref('snowplow_unnested_events') }}

WITH unioned_view AS (

{{ schema_union_all('snowplow_', 'snowplow_unnested_events', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

)

SELECT
  event_id                            AS event_id,
  derived_tstamp::TIMESTAMP           AS behavior_at,
  event                               AS event,
  event_name                          AS event_name,
  se_action                           AS event_action,
  se_category                         AS event_category,
  se_label                            AS event_label,
  se_property                         AS event_property,
  platform                            AS platform,
  gsc_pseudonymized_user_id           AS gsc_pseudonymized_user_id,
  page_urlhost                        AS page_url_host,
  app_id                              AS app_id,
  domain_sessionid                    AS session_id,
  lc_targeturl                        AS link_click_target_url,
  sf_formid                           AS submit_form_id,
  cf_formid                           AS change_form_id,
  cf_type                             AS change_form_type,
  cf_elementid                        AS change_form_element_id,
  ff_elementid                        AS focus_form_element_id,
  ff_nodename                         AS focus_form_node_name,
  br_family                           AS browser_name,
  br_name                             AS browser_major_version,
  br_version                          AS browser_minor_version,
  br_lang                             AS browser_language,
  br_renderengine                     AS browser_engine,
  os_name                             AS os_name,
  os_timezone                         AS os_timezone,
  gsc_environment                     AS environment,
  v_tracker                           AS tracker_version,
  TRY_PARSE_JSON(contexts)::VARIANT   AS contexts,
  dvce_created_tstamp::TIMESTAMP      AS dvce_created_tstamp,
  collector_tstamp::TIMESTAMP         AS collector_tstamp,
  domain_userid                       AS user_snowplow_domain_id,
  domain_sessionidx::INT              AS session_index,
  (page_urlhost || page_urlpath)      AS page_url,
  page_urlscheme                      AS page_url_scheme,
  page_urlpath                        AS page_url_path,
  {{ clean_url('page_urlpath') }}     AS clean_url_path,
  page_urlfragment                    AS page_url_fragment,
  gsc_environment                     AS gsc_environment,
  gsc_extra                           AS gsc_extra,
  gsc_namespace_id                    AS gsc_namespace_id,
  gsc_plan                            AS gsc_plan,
  gsc_google_analytics_client_id      AS gsc_google_analytics_client_id,
  gsc_project_id                      AS gsc_project_id,
  gsc_source                          AS gsc_source,
  os_family                           AS os,
  os_manufacturer                     AS os_manufacturer,
  dvce_type                           AS device_type,
  dvce_ismobile::BOOLEAN              AS is_device_mobile,
  refr_medium                         AS referrer_medium
FROM unioned_view