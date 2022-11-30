with find_current_campaigns as (
    select
    campaign_history.*,
    max(updated_at) over (PARTITION by id order by updated_at desc) as latest_update,
    latest_update = updated_at as is_latest
    from
    raw.google_ads.campaign_history
), current_campaigns as (
    select
    find_current_campaigns.*
    from
    find_current_campaigns
    where is_latest
), find_current_ads as (
    select
    AD_HISTORY.*,
    max(updated_at) over (PARTITION by id order by updated_at desc) as latest_update,
    latest_update = updated_at as is_latest
    from
    raw.google_ads.AD_HISTORY
), current_ads as (
    select
    find_current_ads.*
    from
    find_current_ads
    where is_latest
), find_ad_text as (
    select
    EXPANDED_TEXT_AD_HISTORY.*,
    max(updated_at) over (PARTITION by AD_ID order by updated_at desc) as latest_update,
    latest_update = updated_at as is_latest
    from
    raw.google_ads.EXPANDED_TEXT_AD_HISTORY
), current_ad_text as (
    select
    *
    from
    find_ad_text
    where is_latest
)
select
    /* Campaign */
    
    AD_GROUP_HOURLY_STATS.campaign_id,
    current_campaigns.ad_serving_optimization_status as campaign_ad_serving_optimization_status,
    current_campaigns.advertising_channel_subtype    as campaign_advertising_channel_subtype,
    current_campaigns.advertising_channel_type       as campaign_advertising_channel_type,
    current_campaigns.experiment_type                as campaign_experiment_type,
    current_campaigns.final_url_suffix               as campaign_final_url_suffix,
    current_campaigns.frequency_caps                 as campaign_frequency_caps,
    current_campaigns.name                           as campaign_name,
    current_campaigns.payment_mode                   as campaign_payment_mode,
    current_campaigns.serving_status                 as campaign_serving_status,
    current_campaigns.status                         as campaign_status,
    current_campaigns.end_date                       as campaign_end_date,
    current_campaigns.start_date                     as campaign_start_date,
    
    /* Ad Group */
    
    AD_GROUP_HOURLY_STATS.date                               as date,
    AD_GROUP_HOURLY_STATS.hour,
    AD_GROUP_HOURLY_STATS.id                                 as ad_group_id,
    AD_GROUP_HOURLY_STATS.campaign_base_campaign             as ad_group_campaign_base_campaign,
    AD_GROUP_HOURLY_STATS.click_type                         as ad_group_click_type,
    AD_GROUP_HOURLY_STATS.conversions                        as ad_group_conversions,
    AD_GROUP_HOURLY_STATS.interactions                       as ad_group_interactions,
    AD_GROUP_HOURLY_STATS.average_cpm                        as ad_group_average_cpm,
    AD_GROUP_HOURLY_STATS.device                             as ad_group_device,
    AD_GROUP_HOURLY_STATS.active_view_impressions            as ad_group_active_view_impressions,
    AD_GROUP_HOURLY_STATS.clicks                             as ad_group_clicks,
    AD_GROUP_HOURLY_STATS.active_view_measurable_impressions as ad_group_active_view_measurable_impressions,
    AD_GROUP_HOURLY_STATS.cost_per_conversion                as ad_group_cost_per_conversion,
    AD_GROUP_HOURLY_STATS.active_view_measurability          as ad_group_active_view_measurability,
    AD_GROUP_HOURLY_STATS.average_cpc                        as ad_group_average_cpc,
    AD_GROUP_HOURLY_STATS.ctr                                as ad_group_conversions_value,
    AD_GROUP_HOURLY_STATS.conversions_value                  as ad_group_conversions_value,
    AD_GROUP_HOURLY_STATS.average_cost                       as ad_group_average_cost,
    AD_GROUP_HOURLY_STATS.interaction_rate                   as ad_group_interaction_rate,
    AD_GROUP_HOURLY_STATS.impressions                        as ad_group_impressions,
    AD_GROUP_HOURLY_STATS.active_view_viewability            as ad_group_active_view_viewability,
    AD_GROUP_HOURLY_STATS.active_view_cpm,
    AD_GROUP_HOURLY_STATS.active_view_ctr,
    AD_GROUP_HOURLY_STATS.active_view_measurable_cost_micros,
    AD_GROUP_HOURLY_STATS.base_ad_group,
    AD_GROUP_HOURLY_STATS.conversions_from_interactions_rate,
    AD_GROUP_HOURLY_STATS.cost_micros,
    
    /* Ad Info */
    current_ads.action_items        as ad_action_items,
    current_ads.display_url         as ad_display_url,
    current_ads.final_url_suffix    as ad_final_url_suffix,
    current_ads.final_app_urls      as ad_final_app_urls,
    current_ads.final_mobile_urls   as ad_final_mobile_urls,
    current_ads.final_urls          as ad_final_urls,
    current_ads.status              as ad_status,
    current_ads.type                as ad_type,
    
    /* Ad Text */
    current_ad_text.DESCRIPTION     as ad_text_DESCRIPTION,
    current_ad_text.DESCRIPTION_2   as ad_text_DESCRIPTION_2,
    current_ad_text.headline_part_1 as ad_text_headline_part_1,
    current_ad_text.headline_part_2 as ad_text_headline_part_2,
    current_ad_text.headline_part_3 as ad_text_headline_part_3,
    current_ad_text.path_1          as ad_text_part_1,
    current_ad_text.path_2          as ad_text_part_2
    
from
raw.google_ads.AD_GROUP_HOURLY_STATS
    join current_ads on AD_GROUP_HOURLY_STATS.id = current_ads.AD_GROUP_ID
    left join current_campaigns on AD_GROUP_HOURLY_STATS.campaign_id = current_campaigns.id 
    left join current_ad_text on current_ads.id = current_ad_text.ad_id and current_ads.AD_GROUP_ID = current_ad_text.AD_GROUP_ID
