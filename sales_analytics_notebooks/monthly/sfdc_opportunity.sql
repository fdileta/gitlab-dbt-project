SELECT o.*,
    u.employee_number AS opportunity_owner_employee_number,
    a.billing_country AS account_billing_country,
    upa.billing_country AS upa_billing_country
FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf o
LEFT JOIN prod.workspace_sales.sfdc_users_xf u
    ON o.owner_id = u.user_id
LEFT JOIN prod.restricted_safe_legacy.sfdc_accounts_xf a
    ON a.account_id = o.account_id
LEFT JOIN prod.restricted_safe_legacy.sfdc_accounts_xf upa
    ON upa.account_id = o.ultimate_parent_account_id