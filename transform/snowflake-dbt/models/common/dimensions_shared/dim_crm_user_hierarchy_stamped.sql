{{ config(
    tags=["mnpi_exception"]
) }}


WITH crm_user_hierarchy_stamped AS (

    SELECT
      dim_crm_user_hierarchy_stamped_id,
      fiscal_year,
      dim_crm_opp_owner_sales_segment_stamped_id,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_sales_segment_stamped_grouped,
      dim_crm_opp_owner_geo_stamped_id,
      crm_opp_owner_geo_stamped,
      dim_crm_opp_owner_region_stamped_id,
      crm_opp_owner_region_stamped,
      crm_opp_owner_sales_segment_region_stamped_grouped,
      dim_crm_opp_owner_area_stamped_id,
      crm_opp_owner_area_stamped,
      crm_opp_owner_sales_segment_geo_region_area_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}
)

{{ dbt_audit(
    cte_ref="crm_user_hierarchy_stamped",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2021-01-05",
    updated_date="2022-03-07"
) }}
