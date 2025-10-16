with ranked_loans as (select 
        d.objectid
        ,e.legal_name as deal_name
        ,a.state as deal_stage
        ,l.internal_id as loan_number
        ,l.total_amount as total_loan_amount
        ,case 
             when l.total_amount  <= 5000000 then '$0MM - $5MM'  
             when  l.total_amount > 5000000 and l.total_amount <= 15000000 then '$5MM - $15MM' 
             when l.total_amount  > 15000000 then '$15MM+'  else ' ' end as Loan_Amount_Bracket
        ,case when a.type = 'Application::Bridge' then 'Bridge' else 'Permanent' end as Loan_Type
        ,p.name as product_subtype
        ,t.final_terms_data:term::INTEGER as term
        ,l.properties_count
        ,t.final_terms_data:as_is_value::INTEGER as appraisal_as_is
        ,case when t.final_terms_data:as_is_value::INTEGER is null then (d.property_portfolio_value / l.properties_count ) else (t.final_terms_data:as_is_value::INTEGER / l.properties_count) end  as average_property_value
        ,case when rq.actual_purchase_financing_percent is null then t.final_terms_data:actual_arv_ltv::INTEGER else rq.actual_purchase_financing_percent end as LTV_percent
        ,case when p.amortization_term is null then 'Interest Only' else '30 Years Amortization' end as amortization_type
        ,case 
            when l.pre_payment_penalty = 'none' then 'N/A'
            when l.pre_payment_penalty ='yield_maintenance' then 'Yield Maintenance'
            else 'Yield Maintenance' end as pre_payment_penalty
        ,(l.interest_rate / 100)  as all_in_rate
        ,case when (l.margin_percent / 100) is null then rq.actual_yield_spread_premium_percent else (l.margin_percent / 100)  end as spread
        ,rq.actual_origination_fee_percent as origination_fee 
        ,case 
when a.type = 'Application::Bridge' then l.exit_fee_percent else 0 end as exit_fee_percent
        ,case when (a.state = 'setup_servicing' or a.state = 'closed') then (po.borrower_rate - lp.interest_rate) end as Strip
        ,d.property_bridge_strip__calc_ as strip_hubspot
        ,l.settlement_date
        ,concat(c.first_name, ' ',c.last_name) as loan_officer_name
        ,row_number() over (partition by l.internal_id order by l.settlement_date desc) as row_num
    from ods.ods_los.loans l 
    left join ods.ods_los.applications a on l.application_id = a.id
    left join ods.ods_los.contacts c on a.loan_officer_id = c.id 
    left join ods.ods_los.products p on a.product_id = p.id 
    left join ods.ods_los.entities e on a.entity_id = e.id
    left join ods.ods_los.term_sheets t on a.id = t.application_id
    left join ods_crm.v2_live.objects_deals d on d.property_loan_id = l.internal_id
    left join ods.ods_los.loans_pools lp on lp.loan_id = l.id
    left join ods.ods_los.pools po on lp.pool_id = po.id
    left join ods.ods_los.rate_quotes rq on rq.application_id = a.id
    where a.lo_department = 'pcg'
    and a.state in ('closed', 'setup_servicing'))

    select
    *
    from ranked_loans rl
    where rl.row_num = 1