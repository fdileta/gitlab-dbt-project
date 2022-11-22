WITH source AS (

  SELECT
    age::int AS age,
    job,
    marital,
    education,
    "DEFAULT" AS is_default,
    balance::int AS balance,
    housing,
    loan,
    contact,
    day::int AS day,
    month,
    duration::int AS duration,
    campaign::int AS campaign,
    pdays::int AS pdays,
    previous::int AS previous,
    poutcome,
    y
  FROM {{ source('sheetload', 'toy_marketing_handbook') }}

)

SELECT *
FROM source
