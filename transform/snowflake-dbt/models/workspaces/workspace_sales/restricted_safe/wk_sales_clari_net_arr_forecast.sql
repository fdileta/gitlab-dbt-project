{{ config(
    tags=["mnpi"]
    )
}}

{{ simple_cte([
    ('entries', 'clari_net_arr_entries_source'),
    ('users', 'clari_net_arr_users_source'),
    ('fields', 'clari_net_arr_fields_source'),
    ('time_frames', 'clari_net_arr_time_frames_source')
]) }},

wk_sales_clari_net_arr_forecast AS (
  SELECT
    users.user_full_name,
    users.email,
    users.crm_user_id,
    users.sales_team_role,
    users.parent_role,
    entries.fiscal_quarter,
    fields.field_name,
    time_frames.week_number,
    time_frames.week_start_date,
    time_frames.week_end_date,
    fields.field_type,
    entries.forecast_value,
    entries.is_updated
  FROM
    entries
  INNER JOIN users ON entries.user_id = users.user_id
  INNER JOIN fields ON entries.field_id = fields.field_id
  INNER JOIN time_frames ON entries.time_frame_id = time_frames.time_frame_id
  ORDER BY entries.fiscal_quarter, time_frames.week_number
)

SELECT
  *
FROM
  wk_sales_clari_net_arr_forecast
