WITH source AS (

    SELECT *
    FROM {{ref('xactly_pos_rel_type_hist_source')}}

)

SELECT *
FROM source