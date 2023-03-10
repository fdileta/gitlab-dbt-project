WITH source AS (
    SELECT *
    FROM {{ source('snapshots', 'bizible_attribution_touchpoint_snapshots') }}

), renamed AS (

    SELECT
      id                             AS attribution_touchpoint_id,
      modified_date                  AS modified_date,
      opportunity_id                 AS opportunity_id,
      contact_id                     AS contact_id,
      email                          AS email,
      account_id                     AS account_id,
      user_touchpoint_id             AS user_touchpoint_id,
      visitor_id                     AS visitor_id,
      touchpoint_date                AS touchpoint_date,
      marketing_touch_type           AS marketing_touch_type,
      channel                        AS channel,
      category1                      AS category1,
      category2                      AS category2,
      category3                      AS category3,
      category4                      AS category4,
      category5                      AS category5,
      category6                      AS category6,
      category7                      AS category7,
      category8                      AS category8,
      category9                      AS category9,
      category10                     AS category10,
      category11                     AS category11,
      category12                     AS category12,
      category13                     AS category13,
      category14                     AS category14,
      category15                     AS category15,
      browser_name                   AS browser_name,
      browser_version                AS browser_version,
      platform_name                  AS platform_name,
      platform_version               AS platform_version,
      landing_page                   AS landing_page,
      landing_page_raw               AS landing_page_raw,
      referrer_page                  AS referrer_page,
      referrer_page_raw              AS referrer_page_raw,
      form_page                      AS form_page,
      form_page_raw                  AS form_page_raw,
      form_date                      AS form_date,
      city                           AS city,
      region                         AS region,
      country                        AS country,
      medium                         AS medium,
      web_source                     AS web_source,
      search_phrase                  AS search_phrase,
      ad_provider                    AS ad_provider,
      account_unique_id              AS account_unique_id,
      account_name                   AS account_name,
      advertiser_unique_id           AS advertiser_unique_id,
      advertiser_name                AS advertiser_name,
      site_unique_id                 AS site_unique_id,
      site_name                      AS site_name,
      placement_unique_id            AS placement_unique_id,
      placement_name                 AS placement_name,
      campaign_unique_id             AS campaign_unique_id,
      campaign_name                  AS campaign_name,
      ad_group_unique_id             AS ad_group_unique_id,
      ad_group_name                  AS ad_group_name,
      ad_unique_id                   AS ad_unique_id,
      ad_name                        AS ad_name,
      creative_unique_id             AS creative_unique_id,
      creative_name                  AS creative_name,
      creative_description_1         AS creative_description_1,
      creative_description_2         AS creative_description_2,
      creative_destination_url       AS creative_destination_url,
      creative_display_url           AS creative_display_url,
      keyword_unique_id              AS keyword_unique_id,
      keyword_name                   AS keyword_name,
      keyword_match_type             AS keyword_match_type,
      is_first_touch                 AS is_first_touch,
      is_lead_creation_touch         AS is_lead_creation_touch,
      is_opp_creation_touch          AS is_opp_creation_touch,
      is_closed_touch                AS is_closed_touch,
      stages_touched                 AS stages_touched,
      is_form_submission_touch       AS is_form_submission_touch,
      is_impression_touch            AS is_impression_touch,
      first_click_percentage         AS first_click_percentage,
      last_anon_click_percentage     AS last_anon_click_percentage,
      u_shape_percentage             AS u_shape_percentage,
      w_shape_percentage             AS w_shape_percentage,
      full_path_percentage           AS full_path_percentage,
      custom_model_percentage        AS custom_model_percentage,
      is_deleted                     AS is_deleted,
      row_key                        AS row_key,
      opportunity_row_key            AS opportunity_row_key,
      landing_page_key               AS landing_page_key,
      referrer_page_key              AS referrer_page_key,
      form_page_key                  AS form_page_key,
      account_row_key                AS account_row_key,
      advertiser_row_key             AS advertiser_row_key,
      site_row_key                   AS site_row_key,
      placement_row_key              AS placement_row_key,
      campaign_row_key               AS campaign_row_key,
      ad_row_key                     AS ad_row_key,
      ad_group_row_key               AS ad_group_row_key,
      creative_row_key               AS creative_row_key,
      keyword_row_key                AS keyword_row_key,
      _created_date                  AS _created_date,
      _modified_date                 AS _modified_date,
      _deleted_date                  AS _deleted_date,

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to
    
    FROM source
)

SELECT *
FROM renamed