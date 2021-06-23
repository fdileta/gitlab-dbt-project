WITH zuora_revenue_revenue_contract_line AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_revenue_contract_line')}}

), renamed AS (

    SELECT 
    
      id::VARCHAR                                       AS revenue_contract_line_id,
      rc_id::VARCHAR                                    AS revenue_contract_id,
      type::VARCHAR                                     AS revenue_contract_line_type,
      rc_pob_id::VARCHAR                                AS revenue_contract_performance_obligation_id,
      ext_sll_prc::FLOAT                                AS extended_selling_price,
      ext_fv_prc::FLOAT                                 AS extended_fair_value_price,
      def_amt::FLOAT                                    AS deferred_amount,
      rec_amt::FLOAT                                    AS recognized_amount,
      cv_amt::FLOAT                                     AS carve_amount,
      alctbl_xt_prc::VARCHAR                            AS allocatable_price,
      alctd_xt_prc::VARCHAR                             AS allocated_price,
      bld_def_amt::VARCHAR                              AS billed_deferred_amount,
      bld_rec_amt::VARCHAR                              AS billed_recognized_amount,
      cstmr_nm::VARCHAR                                 AS customer_name,
      so_num::VARCHAR                                   AS sales_order_number,
      so_book_date::DATETIME                            AS sales_order_book_date,
      so_line_num::VARCHAR                              AS sales_order_line_number,
      so_line_id::VARCHAR                               AS sales_order_line_id,
      item_num::VARCHAR                                 AS item_number,
      bndl_cnfg_id::VARCHAR                             AS bundle_configuration_id,
      ord_qty::VARCHAR                                  AS order_quantity,
      inv_qty::VARCHAR                                  AS invoice_quantity,
      ret_qty::VARCHAR                                  AS return_quantity,
      start_date::VARCHAR                               AS revenue_start_date,
      end_date::VARCHAR                                 AS revenue_end_date,
      duration::VARCHAR                                 AS revenue_amortization_duration,
      ext_lst_prc::VARCHAR                              AS list_price,
      batch_id::VARCHAR                                 AS revenue_contract_batch_id,
      curr::VARCHAR                                     AS transactional_currency,
      f_cur::VARCHAR                                    AS functional_currency,
      f_ex_rate::VARCHAR                                AS functional_currency_exchage_rate,
      g_ex_rate::VARCHAR                                AS reporting_currency_exchange_rate,
      disc_amt::FLOAT                                   AS discount_amount,
      disc_pct::FLOAT                                   AS discount_percent,
      alctbl_fn_xt_prc::VARCHAR                         AS allocatable_functional_price,
      ref_doc_line_id::VARCHAR                          AS reference_document_line_id,
      lt_def_acnt::VARCHAR                              AS long_term_deferred_account,
      fv_grp_id::VARCHAR                                AS fair_value_group_id,
      fv_pct::FLOAT                                     AS fair_value_percent,
      fv_prc::FLOAT                                     AS fair_value_price,
      fv_type::VARCHAR                                  AS fair_value_type,
      err_msg::VARCHAR                                  AS error_message,
      def_segments::VARCHAR                             AS deferred_accounting_segment,
      rev_segments::VARCHAR                             AS revenue_accounting_segment,
      atr1::VARCHAR                                     AS revenue_contract_line_attribute_1,
      atr2::VARCHAR                                     AS revenue_contract_line_attribute_2,
      atr3::VARCHAR                                     AS revenue_contract_line_attribute_3,
      atr4::VARCHAR                                     AS revenue_contract_line_attribute_4,
      atr5::VARCHAR                                     AS revenue_contract_line_attribute_5,
      atr6::VARCHAR                                     AS revenue_contract_line_attribute_6,
      atr7::VARCHAR                                     AS revenue_contract_line_attribute_7,
      atr8::VARCHAR                                     AS revenue_contract_line_attribute_8,
      atr9::VARCHAR                                     AS revenue_contract_line_attribute_9,
      atr10::VARCHAR                                    AS revenue_contract_line_attribute_10,
      atr11::VARCHAR                                    AS revenue_contract_line_attribute_11,
      atr12::VARCHAR                                    AS revenue_contract_line_attribute_12,
      atr13::VARCHAR                                    AS revenue_contract_line_attribute_13,
      atr14::VARCHAR                                    AS revenue_contract_line_attribute_14,
      atr15::VARCHAR                                    AS revenue_contract_line_attribute_15,
      atr16::VARCHAR                                    AS revenue_contract_line_attribute_16,
      atr17::VARCHAR                                    AS revenue_contract_line_attribute_17,
      atr18::VARCHAR                                    AS revenue_contract_line_attribute_18,
      atr19::VARCHAR                                    AS revenue_contract_line_attribute_19,
      atr20::VARCHAR                                    AS revenue_contract_line_attribute_20,
      atr21::VARCHAR                                    AS revenue_contract_line_attribute_21,
      atr22::VARCHAR                                    AS revenue_contract_line_attribute_22,
      atr23::VARCHAR                                    AS revenue_contract_line_attribute_23,
      atr24::VARCHAR                                    AS revenue_contract_line_attribute_24,
      atr25::VARCHAR                                    AS revenue_contract_line_attribute_25,
      atr26::VARCHAR                                    AS revenue_contract_line_attribute_26,
      atr27::VARCHAR                                    AS revenue_contract_line_attribute_27,
      atr28::VARCHAR                                    AS revenue_contract_line_attribute_28,
      atr29::VARCHAR                                    AS revenue_contract_line_attribute_29,
      atr30::VARCHAR                                    AS revenue_contract_line_attribute_30,
      atr31::VARCHAR                                    AS revenue_contract_line_attribute_31,
      atr32::VARCHAR                                    AS revenue_contract_line_attribute_32,
      atr33::VARCHAR                                    AS revenue_contract_line_attribute_33,
      atr34::VARCHAR                                    AS revenue_contract_line_attribute_34,
      atr35::VARCHAR                                    AS revenue_contract_line_attribute_35,
      atr36::VARCHAR                                    AS revenue_contract_line_attribute_36,
      atr37::VARCHAR                                    AS revenue_contract_line_attribute_37,
      atr38::VARCHAR                                    AS revenue_contract_line_attribute_38,
      atr39::VARCHAR                                    AS revenue_contract_line_attribute_39,
      atr40::VARCHAR                                    AS revenue_contract_line_attribute_40,
      atr41::VARCHAR                                    AS revenue_contract_line_attribute_41,
      atr42::VARCHAR                                    AS revenue_contract_line_attribute_42,
      atr43::VARCHAR                                    AS revenue_contract_line_attribute_43,
      atr44::VARCHAR                                    AS revenue_contract_line_attribute_44,
      atr45::VARCHAR                                    AS revenue_contract_line_attribute_45,
      atr46::VARCHAR                                    AS revenue_contract_line_attribute_46,
      atr47::VARCHAR                                    AS revenue_contract_line_attribute_47,
      atr48::VARCHAR                                    AS revenue_contract_line_attribute_48,
      atr49::VARCHAR                                    AS revenue_contract_line_attribute_49,
      atr50::VARCHAR                                    AS revenue_contract_line_attribute_50,
      atr51::VARCHAR                                    AS revenue_contract_line_attribute_51,
      atr52::VARCHAR                                    AS revenue_contract_line_attribute_52,
      atr53::VARCHAR                                    AS revenue_contract_line_attribute_53,
      atr54::VARCHAR                                    AS revenue_contract_line_attribute_54,
      atr55::VARCHAR                                    AS revenue_contract_line_attribute_55,
      atr56::VARCHAR                                    AS revenue_contract_line_attribute_56,
      atr57::VARCHAR                                    AS revenue_contract_line_attribute_57,
      atr58::VARCHAR                                    AS revenue_contract_line_attribute_58,
      atr59::VARCHAR                                    AS revenue_contract_line_attribute_59,
      atr60::VARCHAR                                    AS revenue_contract_line_attribute_60,
      num1::VARCHAR                                     AS revenue_contract_line_number_1,
      num2::VARCHAR                                     AS revenue_contract_line_number_2,
      num3::VARCHAR                                     AS revenue_contract_line_number_3,
      num4::VARCHAR                                     AS revenue_contract_line_number_4,
      num5::VARCHAR                                     AS revenue_contract_line_number_5,
      num6::VARCHAR                                     AS revenue_contract_line_number_6,
      num7::VARCHAR                                     AS revenue_contract_line_number_7,
      num8::VARCHAR                                     AS revenue_contract_line_number_8,
      num9::VARCHAR                                     AS revenue_contract_line_number_9,
      num10::VARCHAR                                    AS revenue_contract_line_number_10,
      num11::VARCHAR                                    AS revenue_contract_line_number_11,
      num12::VARCHAR                                    AS revenue_contract_line_number_12,
      num13::VARCHAR                                    AS revenue_contract_line_number_13,
      num14::VARCHAR                                    AS revenue_contract_line_number_14,
      num15::VARCHAR                                    AS revenue_contract_line_number_15,
      date1::DATETIME                                   AS revenue_contract_line_date_1,
      date2::DATETIME                                   AS revenue_contract_line_date_2,
      date3::DATETIME                                   AS revenue_contract_line_date_3,
      date4::DATETIME                                   AS revenue_contract_line_date_4,
      date5::DATETIME                                   AS revenue_contract_line_date_5,
      model_id::VARCHAR                                 AS model_id,
      unschd_adj::VARCHAR                               AS unscheduled_adjustment,
      posted_pct::FLOAT                                 AS posted_percent,
      rel_pct::FLOAT                                    AS released_percent,
      term::VARCHAR                                     AS revenue_contract_line_term,
      vc_type_id::VARCHAR                               AS variable_consideration_type_id,
      po_num::VARCHAR                                   AS purchase_order_number,
      quote_num::VARCHAR                                AS quote_number,
      schd_ship_dt::DATETIME                            AS scheduled_ship_date,
      ship_dt::DATETIME                                 AS ship_date,
      sales_rep_name::VARCHAR                           AS sales_representative_name,
      cust_num::VARCHAR                                 AS customer_number,
      prod_ctgry::VARCHAR                               AS product_category,
      prod_class::VARCHAR                               AS product_class,
      prod_fmly::VARCHAR                                AS product_family,
      prod_ln::VARCHAR                                  AS product_line,
      business_unit::VARCHAR                            AS business_unit,
      ct_mod_date::DATETIME                             AS contract_modification_date,
      ct_num::VARCHAR                                   AS contract_number,
      ct_date::DATETIME                                 AS contract_date,
      ct_line_num::VARCHAR                              AS contract_line_number,
      ct_line_id::VARCHAR                               AS contract_line_id,
      cum_cv_amt::VARCHAR                               AS cumulative_carve_amount,
      cum_alctd_amt::VARCHAR                            AS cumulative_allocated_amount,
      comments::VARCHAR                                 AS revenue_contract_line_comment,
      prnt_ln_id::VARCHAR                               AS parent_revenue_contract_line_id,
      prnt_ref_ln_id::VARCHAR                           AS parent_reference_line_id,
      cv_eligible_flag::VARCHAR                         AS is_carve_eligible,
      return_flag::VARCHAR                              AS is_return,
      within_fv_range_flag::VARCHAR                     AS is_within_fair_value_range,
      stated_flag::VARCHAR                              AS is_stated,
      standalone_flag::VARCHAR                          AS is_standalone,
      disc_adj_flag::VARCHAR                            AS is_discount_adjustment,
      approval_status_flag::VARCHAR                     AS approval_status,
      fv_eligible_flag::VARCHAR                         AS is_fair_value_eligible,
      manual_fv_flag::VARCHAR                           AS is_manual_fair_value,
      rssp_calc_type::VARCHAR                           AS rssp_calculation_type,
      unbill_flag::VARCHAR                              AS is_unbilled,
      manual_crtd_flag::VARCHAR                         AS is_manual_created,
      vc_clearing_flag::VARCHAR                         AS is_variable_consideration_clearing,
      mje_line_flag::VARCHAR                            AS is_manual_journal_entry_line,
      update_or_insert_flag::VARCHAR                    AS is_update_or_insert,
      delink_lvl_flag::VARCHAR                          AS delink_level,
      CONCAT(crtd_prd_id::VARCHAR, '01')                AS created_period_id,
      book_id::VARCHAR                                  AS book_id,
      client_id::VARCHAR                                AS client_id,
      sec_atr_val::VARCHAR                              AS security_attribute_value,
      crtd_by::VARCHAR                                  AS revenue_contract_line_created_by,
      crtd_dt::DATETIME                                 AS revenue_contract_line_created_date,
      updt_by::VARCHAR                                  AS revenue_contract_line_updated_by,
      updt_dt::DATETIME                                 AS revenue_contract_line_updated_date,
      incr_updt_dt::VARCHAR                             AS incremental_update_date,
      offset_segments::VARCHAR                          AS offset_accounting_segment,
      sob_id::VARCHAR                                   AS set_of_books_id,
      fv_date::DATETIME                                 AS fair_value_date,
      orig_fv_date::DATETIME                            AS original_fair_value_date,
      vc_amt::FLOAT                                     AS varaiable_consideration_amount,
      unit_sell_prc::FLOAT                              AS unit_sell_price,
      unit_list_prc::FLOAT                              AS unit_list_price,
      impair_retrieve_amt::FLOAT                        AS impairment_retrieve_amount,
      bndl_prnt_id::VARCHAR                             AS bundle_parent_id,
      company_code::VARCHAR                             AS company_code,
      cancel_flag::VARCHAR                              AS is_cancelled,
      below_fv_prc::FLOAT                               AS below_fair_value_price,
      above_fv_prc::FLOAT                               AS above_fair_value_price,
      fv_tmpl_id::VARCHAR                               AS fair_value_template_id,
      fv_expr::VARCHAR                                  AS fair_value_expiration,
      below_mid_pct::FLOAT                              AS below_mid_percent,
      above_mid_pct::FLOAT                              AS above_mid_percent,
      ic_account::VARCHAR                               AS intercompany_account,
      ca_account::VARCHAR                               AS contract_asset_account,
      ci_account::VARCHAR                               AS ci_account,
      al_account::VARCHAR                               AS al_account,
      ar_account::VARCHAR                               AS ar_account,
      contra_ar_acct::VARCHAR                           AS contra_ar_account,
      payables_acct::VARCHAR                            AS payables_account,
      lt_def_adj_acct::VARCHAR                          AS long_term_deferred_adjustment_account,
      ub_liab_acct::VARCHAR                             AS ub_liability_account,
      alloc_rec_hold_flag::VARCHAR                      AS is_allocation_recognition_hold,
      alloc_schd_hold_flag::VARCHAR                     AS is_allocation_schedule_hold,
      alloc_trtmt_flag::VARCHAR                         AS is_allocation_treatment,
      contra_entry_flag::VARCHAR                        AS is_contra_entry,
      conv_wfall_flag::VARCHAR                          AS is_conv_waterfall,
      ct_mod_code_flag::VARCHAR                         AS contract_modification_code,
      impairment_type_flag::VARCHAR                     AS impairment_type,
      previous_fv_flag::VARCHAR                         AS previous_fair_value,
      reclass_flag::VARCHAR                             AS is_reclass,
      rev_rec_hold_flag::VARCHAR                        AS is_revenue_recognition_hold,
      rev_schd_hold_flag::VARCHAR                       AS is_revenue_schedule_hold,
      rev_schd_flag::VARCHAR                            AS is_revevnue_schedule,
      trnsfr_hold_flag::VARCHAR                         AS is_transfer_hold,
      alloc_delink_flag::VARCHAR                        AS is_allocation_delink,
      cancel_by_rord_flag::VARCHAR                      AS is_canceled_by_reduction_order,
      cv_eligible_lvl2_flag::VARCHAR                    AS is_level_2_carve_eligible,
      rc_level_range_flag::VARCHAR                      AS revenue_contract_level,
      rssp_fail_flag::VARCHAR                           AS is_rssp_failed,
      vc_eligible_flag::VARCHAR                         AS is_variable_consideration_eligible,
      ghost_line_flag::VARCHAR                          AS is_ghost_line,
      initial_ct_flag::VARCHAR                          AS is_initial_contract,
      material_rights_flag::VARCHAR                     AS is_material_rights,
      ramp_up_flag::VARCHAR                             AS is_ramp_up,
      lt_def_cogs_acct::VARCHAR                         AS long_term_deferred_cogs_account,
      lt_ca_account::VARCHAR                            AS long_term_ca_account,
      tot_bgd_hrs::FLOAT                                AS total_budget_hours,
      tot_bgd_cst::FLOAT                                AS total_budget_cost,
      fcst_date::DATETIME                               AS forecast_date,
      link_identifier::VARCHAR                          AS link_identifier,
      prod_life_term::VARCHAR                           AS product_life_term,
      ramp_identifier::VARCHAR                          AS ramp_identifier,
      mr_line_id::VARCHAR                               AS material_rights_line_id,
      price_point::VARCHAR                              AS price_point,
      tp_pct_ssp::FLOAT                                 AS transaction_price_ssp_percent,
      orig_quantity::FLOAT                              AS original_quantity,
      split_ref_doc_line_id::VARCHAR                    AS split_reference_document_line_id,
      overstated_amt::FLOAT                             AS overstated_amount,
      ovst_lst_amt::FLOAT                               AS overstated_list_price_amount,
      ref_rc_id::VARCHAR                                AS reference_revenue_contract_id,
      split_flag::VARCHAR                               AS is_split,
      ord_orch_flag::VARCHAR                            AS is_ord_orch,
      upd_model_id_flag::VARCHAR                        AS updated_model_id,
      new_pob_flag::VARCHAR                             AS is_new_performance_obligation,
      mr_org_prc::FLOAT                                 AS material_rights_org_percent,
      action_type::VARCHAR                              AS action_type,
      pord_def_amt::FLOAT                               AS pord_deferred_amount,
      pord_rec_amt::FLOAT                               AS pord_recognized_amount,
      net_sll_prc::FLOAT                                AS net_sell_price,
      net_lst_prc::FLOAT                                AS net_list_price,
      full_cm_flag::VARCHAR                             AS full_cm_flag,
      skip_ct_mod_flag::VARCHAR                         AS is_skip_contract_modification,
      step1_rc_level_range_flag::VARCHAR                AS step_1_revenue_contract_level_range,
      impairment_exception_flag::VARCHAR                AS is_impairment_exception,
      pros_defer_flag::VARCHAR                          AS is_pros_deferred,
      manual_so_flag::VARCHAR                           AS is_manual_sales_order,
      zero_doll_rec_flag::VARCHAR                       AS is_zero_dollar_recognition,
      cv_amt_imprtmt::VARCHAR                           AS carve_amount_imprtmt,
      full_pord_disc_flag::VARCHAR                      AS is_full_pord_discount,
      zero_dollar_rord_flag::VARCHAR                    AS is_zero_dollar_reduction_order,
      subscrp_id::VARCHAR                               AS subscription_id,
      subscrp_name::VARCHAR                             AS subscription_name,
      subscrp_version::VARCHAR                          AS subscription_version,
      subscrp_start_date::DATETIME                      AS subscription_start_date,
      subscrp_end_date::DATETIME                        AS subscription_end_date,
      subscrp_owner::VARCHAR                            AS subscription_owner,
      invoice_owner::VARCHAR                            AS invoice_owner,
      rp_id::VARCHAR                                    AS rate_plan_id,
      rp_name::VARCHAR                                  AS rate_plan_name,
      rpc_num::VARCHAR                                  AS rate_plan_charge_number,
      rpc_name::VARCHAR                                 AS rate_plan_charge_name,
      rpc_version::VARCHAR                              AS rate_plan_charge_version,
      rpc_model::VARCHAR                                AS rate_plan_charge_model,
      rpc_type::VARCHAR                                 AS rate_plan_charge_type,
      rpc_trigger_evt::VARCHAR                          AS rate_plan_charge_trigger_event,
      rpc_segment::VARCHAR                              AS rate_plan_charge_segment,
      rpc_id::VARCHAR                                   AS rate_plan_charge_id,
      orig_rpc_id::VARCHAR                              AS original_rate_plan_charge_id,
      subscrp_type::VARCHAR                             AS subscription_type,
      charge_level::VARCHAR                             AS charge_level,
      amendment_id::VARCHAR                             AS amendment_id,
      amendment_type::VARCHAR                           AS amendment_type,
      amendment_reason::VARCHAR                         AS amendment_reason,
      order_id::VARCHAR                                 AS order_id,
      order_item_id::VARCHAR                            AS order_item_id,
      order_action_id::VARCHAR                          AS order_action_id,
      account_id::VARCHAR                               AS billing_account_id,
      product_id::VARCHAR                               AS product_id,
      product_rp_id::VARCHAR                            AS product_rate_plan_id,
      product_rpc_id::VARCHAR                           AS product_rate_plan_charge_id,
      charge_crtd_date::DATETIME                        AS charge_created_date,
      charge_last_updt_date::DATETIME                   AS charge_last_updated_date,
      bill_id::VARCHAR                                  AS revenue_contract_bill_id,
      bill_item_id::VARCHAR                             AS revenue_contract_bill_item_id,
      zbill_batch_id::VARCHAR                           AS zbilling_batch_id,
      ramp_deal_ref::VARCHAR                            AS ramp_deal_id,
      seq_num::VARCHAR                                  AS sequence_number,
      avg_prcing_mthd::VARCHAR                          AS average_pricing_method,
      prc_frmt::VARCHAR                                 AS percent_format,
      prnt_chrg_segment::VARCHAR                        AS parent_charge_segment,
      prnt_chrg_num::VARCHAR                            AS parent_charge_number,
      entity_id::VARCHAR                                AS entity_id,
      ssp_sll_prc::FLOAT                                AS ssp_sell_price,
      ssp_lst_prc::FLOAT                                AS ssp_list_price,
      subscrp_trm_st_dt::DATETIME                       AS subscription_term_start_date,
      subscrp_trm_end_dt::DATETIME                      AS subscription_term_end_date,
      subscrp_term_num::VARCHAR                         AS subscrp_term_number,
      old_sell_prc::FLOAT                               AS old_sell_price,
      rstrct_so_val_upd_flag::VARCHAR                   AS is_restricted_sales_order_value_update,
      zbilling_cmplte_flag::VARCHAR                     AS is_zbilling_complete,
      zbil_unschd_adj_flag::VARCHAR                     AS is_zbilling_unscheduled_adjustment,
      zbill_cancel_line_flag::VARCHAR                   AS is_zbillling_cancelled_line,
      non_distinct_pob_flag::VARCHAR                    AS is_non_distinct_performance_obligation,
      zbill_ctmod_rule_flag::VARCHAR                    AS is_zbilling_contract_modification_rule,
      sys_inv_exist_flag::VARCHAR                       AS is_system_inv_exist,
      zbill_manual_so_flag::VARCHAR                     AS is_zbillling_manual_sales_order,
      so_term_change_flag::VARCHAR                      AS is_sales_order_term_change,
      ramp_alloc_pct::FLOAT                             AS ramp_allocation_percent,
      ramp_alctbl_prc::FLOAT                            AS ramp_allocatable_percent,
      ramp_alctd_prc::FLOAT                             AS ramp_allocted_percent,
      ramp_cv_amt::FLOAT                                AS ramp_carve_amount,
      ramp_cum_cv_amt::FLOAT                            AS ramp_cumulative_carve_amount,
      ramp_cum_alctd_amt::FLOAT                         AS ramp_cumulative_allocated_amount,
      ovg_exist_flag::VARCHAR                           AS is_overage_exists,
      zbill_ramp_flag::VARCHAR                          AS is_zbilling_ramp,
      update_by_rord_flag::VARCHAR                      AS is_updated_by_reduction_order,
      unbilled_evergreen_flag::VARCHAR                  AS is_unbilled_evergreen,
      k2_batch_id::VARCHAR                              AS k2_batch_id,
      reason_code::VARCHAR                              AS reason_code,
      CONCAT(updt_prd_id::VARCHAR, '01')                AS revenue_contract_line_updated_period_id,
      ramp_id::VARCHAR                                  AS ramp_id,
      CONCAT(unbl_rvsl_prd::VARCHAR, '01')              AS unbilled_reversal_period,
      ramp_cv_chg_flag::VARCHAR                         AS is_ramp_carve,
      zero_flip_flag::VARCHAR                           AS is_zero_f,
      pros_decrse_prc_flag::VARCHAR                     AS is_pros_decrse_p,
      CONCAT(defer_prd_id::VARCHAR, '01')               AS deferred_period_id

    FROM zuora_revenue_revenue_contract_line

)

SELECT * 
FROM renamed