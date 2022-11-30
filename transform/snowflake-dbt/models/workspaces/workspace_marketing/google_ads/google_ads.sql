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
), find_current_ad_groups as (
    select
    ad_group_history.*,
    max(updated_at) over (PARTITION by id order by updated_at desc) as latest_update,
    latest_update = updated_at as is_latest
    from
    raw.google_ads.ad_group_history
), current_ad_group as (
    select
    find_current_ad_groups.*,
    max(updated_at) over (PARTITION by id order by updated_at desc) as latest_update,
    latest_update = updated_at as is_latest
    from
    find_current_ad_groups
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
    /* Campaign Info */
    
    current_campaigns.id                             as campaign_id,
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
    
    /* Ad Group Info */
    current_ad_group.NAME           as ad_group_name,
    current_ad_group.STATUS         as ad_group_status,
    current_ad_group.TYPE           as ad_group_type,
    
    /* Ad Info */
    current_ads.action_items        as ad_action_items,
    current_ads.display_url         as ad_display_url,
    current_ads.final_url_suffix    as ad_final_url_suffix,
    current_ads.final_app_urls      as ad_final_app_urls,
    current_ads.final_mobile_urls   as ad_final_mobile_urls,
    current_ads.final_urls          as ad_final_urls,
    current_ads.status              as ad_status,
    current_ads.type                as ad_type,
    
    
    /* Ad Stats */
    AD_STATS.date,
    AD_STATS.DEVICE,
    AD_STATS.CONVERSIONS,
    AD_STATS.interactions,
    AD_STATS.IMPRESSIONS,
    AD_STATS.CLICKS,
    AD_STATS.CONVERSIONS_VALUE,
    AD_STATS.VIDEO_VIEWS,
    AD_STATS.ACTIVE_VIEW_MEASURABLE_IMPRESSIONS,
    AD_STATS.ACTIVE_VIEW_MEASURABLE_COST_MICROS,
    AD_STATS.ACTIVE_VIEW_MEASURABILITY,
    AD_STATS.COST_MICROS,
    
    
    /* Ad Text */
    current_ad_text.DESCRIPTION     as ad_text_DESCRIPTION,
    current_ad_text.DESCRIPTION_2   as ad_text_DESCRIPTION_2,
    current_ad_text.headline_part_1 as ad_text_headline_part_1,
    current_ad_text.headline_part_2 as ad_text_headline_part_2,
    current_ad_text.headline_part_3 as ad_text_headline_part_3,
    current_ad_text.path_1          as ad_text_part_1,
    current_ad_text.path_2          as ad_text_part_2
    
from
raw.google_ads.AD_STATS
    join current_ads on AD_STATS.ad_id = current_ads.ID and AD_STATS.ad_group_id = current_ads.ad_group_id
    left join current_ad_text on current_ads.id = current_ad_text.ad_id and current_ads.AD_GROUP_ID = current_ad_text.AD_GROUP_ID
    left join current_campaigns on AD_STATS.campaign_id = current_campaigns.id 
    left join current_ad_group on ad_stats.ad_group_id = current_ad_group.id 
