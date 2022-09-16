{{ config(alias='sfdc_accounts_xf') }}

SELECT * FROM {{ref('sfdc_accounts_xf')}}
--FROM PROD.restricted_safe_legacy.sfdc_accounts_xf