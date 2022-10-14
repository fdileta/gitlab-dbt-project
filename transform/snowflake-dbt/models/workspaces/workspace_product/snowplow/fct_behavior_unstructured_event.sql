{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = false
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unstructured_events_all'),
    ('dim_event', 'dim_behavior_event'),
    ('dim_page', 'dim_behavior_website_page')
    ])
}}

, unstruct_event AS (

    SELECT
      event_id,
      derived_tstamp                                    AS behavior_at,
      event_name,
      gsc_pseudonymized_user_id,
      {{ clean_url('page_url_path') }}                  AS clean_url_path,
      page_url_host,
      app_id,
      session_id,
      lc_targeturl                                      AS link_click_target_url,
      sf_formid                                         AS submit_form_id,
      cf_formid                                         AS change_form_id,
      cf_type                                           AS change_form_type,
      cf_elementid                                      AS change_form_element_id,
      ff_elementid                                      AS focus_form_element_id,
      ff_nodename                                       AS focus_form_node_name
    FROM events
    WHERE derived_tstamp >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

    AND derived_tstamp > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}
)

, unstructured_event_with_dims AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','behavior_at']) }} AS fct_behavior_unstructured_sk,

      -- Natural Key
      event_id,
      session_id,

      -- Surrogate Keys
      dim_event.dim_behavior_event_sk,
      dim_page.dim_behavior_website_page_sk,

      --Time Attributes
      behavior_at,

      -- Google Key
      gsc_pseudonymized_user_id,

      -- Attributes
      link_click_target_url,
      submit_form_id,
      change_form_id,
      change_form_type,
      change_form_element_id,
      focus_form_element_id,
      focus_form_node_name
    FROM unstruct_event
    INNER JOIN dim_event 
      ON unstruct_event.event_name = dim_event.event_name
    INNER JOIN dim_page 
      ON unstruct_event.clean_url_path = dim_page.clean_url_path 
        AND unstruct_event.page_url_host = dim_page.page_url_host
        AND unstruct_event.app_id = dim_page.app_id
      
)

{{ dbt_audit(
    cte_ref="unstructured_event_with_dims",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-27",
    updated_date="2022-09-27"
) }}