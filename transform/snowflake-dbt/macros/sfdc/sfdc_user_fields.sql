{%- macro sfdc_user_fields(model_type) %}

{{ simple_cte([
    ('sfdc_user_roles_source','sfdc_user_roles_source'),
    ('dim_date','dim_date'),
    ('sfdc_users_source', 'sfdc_users_source'),
    ('sfdc_user_snapshots_source', 'sfdc_user_snapshots_source')
]) }}

, sheetload_mapping_sdr_sfdc_bamboohr_source AS (

    SELECT *
    FROM {{ ref('sheetload_mapping_sdr_sfdc_bamboohr_source') }}

{%- if model_type == 'snapshot' %}

), snapshot_dates AS (

    SELECT *
    FROM dim_date
    WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}
{%- endif %}

), sfdc_users AS (

    SELECT 
      {%- if model_type == 'live' %}
        *
      {%- elif model_type == 'snapshot' %}
      {{ dbt_utils.surrogate_key(['sfdc_user_snapshots_source.user_id','snapshot_dates.date_id'])}}    AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                           AS snapshot_id,
      sfdc_user_snapshots_source.*
      {%- endif %}
    FROM
      {%- if model_type == 'live' %}
      sfdc_users_source
      {%- elif model_type == 'snapshot' %}
      sfdc_user_snapshots_source
      INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= sfdc_user_snapshots_source.dbt_valid_from
        AND snapshot_dates.date_actual < COALESCE(sfdc_user_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}

), final AS (

    SELECT
      {%- if model_type == 'live' %}
  
      {%- elif model_type == 'snapshot' %}
      sfdc_users.crm_user_snapshot_id,
      sfdc_users.snapshot_id,
      {%- endif %}
      sfdc_users.user_id                                                                                                              AS dim_crm_user_id,
      sfdc_users.employee_number,
      sfdc_users.name                                                                                                                 AS user_name,
      sfdc_users.title,
      sfdc_users.department,
      sfdc_users.team,
      sfdc_users.manager_id,
      sfdc_users.manager_name,
      sfdc_users.user_email,
      sfdc_users.is_active,
      sfdc_users.start_date,
      sfdc_users.user_role_id,
      sfdc_users.user_role_type,
      sfdc_user_roles_source.name                                                                                                     AS user_role_name,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_segment']) }}                                                                      AS dim_crm_user_sales_segment_id,
      sfdc_users.user_segment                                                                                                         AS crm_user_sales_segment,
      sfdc_users.user_segment_grouped                                                                                                 AS crm_user_sales_segment_grouped,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_geo']) }}                                                                          AS dim_crm_user_geo_id,
      sfdc_users.user_geo                                                                                                             AS crm_user_geo,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_region']) }}                                                                       AS dim_crm_user_region_id,
      sfdc_users.user_region                                                                                                          AS crm_user_region,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_area']) }}                                                                         AS dim_crm_user_area_id,
      sfdc_users.user_area                                                                                                            AS crm_user_area,
      COALESCE(
               sfdc_users.user_segment_geo_region_area,
               CONCAT(sfdc_users.user_segment,'-' , sfdc_users.user_geo, '-', sfdc_users.user_region, '-', sfdc_users.user_area)
               )                                                                                                                      AS crm_user_sales_segment_geo_region_area,
      sfdc_users.user_segment_region_grouped                                                                                          AS crm_user_sales_segment_region_grouped,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_segment                                                                          AS sdr_sales_segment,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region,
      sfdc_users.created_date
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles_source
      ON sfdc_users.user_role_id = sfdc_user_roles_source.id
    LEFT JOIN sheetload_mapping_sdr_sfdc_bamboohr_source
      ON sfdc_users.user_id = sheetload_mapping_sdr_sfdc_bamboohr_source.user_id

)

{%- endmacro %}