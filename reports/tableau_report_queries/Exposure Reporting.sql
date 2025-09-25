with exposure_data as (select 
    c.id as contact_id
    ,current_timestamp
    ,b.primary
    ,concat(c.first_name, ' ',c.last_name) as borrower_name
    ,l.payment_status
    ,split_part(a.type,'::',2) as product_type
    ,l.total_amount
    ,l.internal_id
    ,a.lo_department
    ,cb.investor as closed_bridge_investor
    ,cb.maturity_date as closed_bridge_maturity_date
    ,cb.property_address as closed_bridge_property_address
    ,cb.arv as closed_bridge_arv
    ,cb.rehab_size as closed_bridge_rehab_size
    ,cb.initial_interest_reserves as closed_bridge_initial_interest_reserves
    ,cb.loan_status as closed_bridge_loan_status
    ,cb.term as closed_bridge_term
    ,cb.loan_number as closed_bridge_loan_number
    ,cb.current_loan_amount as closed_bridge_current_loan_amount
    ,cb.purchase_price as closed_bridge_purchase_price
    ,cb.total_loan_amount as closed_bridge_total_loan_amount
    ,cb.closing_date as closed_bridge_closing_date
    ,cr.number_units as closed_rental_number_units
    ,cr.monthly_hoa as closed_rental_monthly_hoa
    ,cr.monthly_taxes as closed_rental_monthly_taxes
    ,cr.market_rent_amount as closed_rental_market_rent_amount
    ,cr.current_property_type as closed_rental_current_property_type
    ,cr.closing_date as closed_rental_closing_date
    ,cr.interest_rate as closed_rental_interest_rate
    ,cr.loan_number as closed_rental_loan_number
    ,cr.appraisal_value as closed_rental_appraisal_value
    ,cr.dscr as closed_rental_dscr
    ,cr.original_mortgage_amount as closed_rental_original_mortgage_amount
    ,cr.payment_status as closed_rental_payment_status
    ,cr.monthly_insurances as closed_rental_monthly_insurances
    ,cr.p_and_i as closed_rental_p_and_i
    ,cr.property_address as closed_rental_property_address
    ,cr.current_rent_amount as closed_rental_current_rent_amount
    ,cr.interest_only_or_amortized as closed_rental_interest_only_or_amortized
    ,pb.loan_purpose as paidoff_bridge_loan_purpose
    ,pb.rehab_size as paidoff_bridge_rehab_size
    ,pb.closing_date as paidoff_bridge_closing_date
    ,pb.property_address as paidoff_bridge_property_address
    ,pb.total_loan_amount as paidoff_bridge_total_loan_amount
    ,pb.loan_number as paidoff_bridge_loan_number
    ,pb.arv as paidoff_bridge_arv
    ,pb.purchase_price as paidoff_bridge_purchase_price
    ,ab.ltc as active_bridge_ltc
    ,ab.property_address as active_bridge_property_address
    ,ab.total_loan_amount as active_bridge_total_loan_amount
    ,ab.arv as active_bridge_arv
    ,ab.loan_number as active_bridge_loan_number
    ,ab.rehab_size as active_bridge_rehab_size
    ,ab.purchase_price as active_bridge_purchase_price
    ,ab.loan_to_arv as active_bridge_loan_to_arv
    ,ab.closing_date as active_bridge_closing_date
    ,dp.ops_channel
    from ods.ods_los.borrowers b 
    left join ods.ods_los.applications a on a.id = b.application_id
    left join ods.ods_los.contacts c on c.id = b.contact_id
    left join ods.ods_los.loans l on l.application_id = b.application_id
    left join ods.ods_los_canned_reports.closed_bridge cb on cb.loan_number = l.internal_id
    left join ods.ods_los_canned_reports.closed_rental cr on cr.loan_number = l.internal_id
    left join ods.ods_los_canned_reports.paidoff_bridge pb on pb.loan_number = l.internal_id
    left join ods.ods_los_canned_reports.active_bridge ab on ab.loan_number = l.internal_id
    left join ods.ods_sales_roster.deal_product_channel dp on dp.internal_id = l.internal_id
    where a.state in ('setup_servicing','closed')),


    borrower_counts as (
    select
    ed.contact_id as  borrower_count_id
    ,sum(case when ed.primary = true then 1 else 0 end) as primary_counts 
    from exposure_data ed 
    group by borrower_count_id
    ),


    partition as (select
    ed.*
    ,row_number() over(partition by ed.internal_id order by bc.primary_counts desc) as rn
    from exposure_data ed 
    left join borrower_counts bc on bc.borrower_count_id = ed.contact_id)


    select
    *
    from partition p

    where p.rn = 1