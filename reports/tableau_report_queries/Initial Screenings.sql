select distinct
    subquery.internal_id as loan_number,
    subquery.id as application_number,
    subquery.address as property_address,
    subquery.appraisal_paid_at as appraisal_paid_date,
    subquery.settlement_date as settlement_date,
    subquery.product_type,
    subquery.loan_officer_name,
    subquery.department
from (
    select applications.id, 
           loans.internal_id, 
           properties.address, 
           loans.appraisal_paid_at, 
           loans.settlement_date,
           users.department,
           deal_product_channel.ops_product as product_type,
           concat(contacts.first_name, ' ', contacts.last_name) as loan_officer_name,
           row_number() over (partition by applications.id order by loans.appraisal_paid_at desc) as row_num
    from ods.ods_los.applications
    inner join ods.ods_los.loans 
        on loans.application_id = applications.id
    inner join ods.ods_los.properties 
        on properties.deleted_at is null 
        and properties.loan_id = loans.id
    inner join ods.ods_los.property_valuations 
        on property_valuations.property_id = properties.id
    inner join ods.ods_los.users on users.contact_id = applications.loan_officer_id
    left join ods.ods_sales_roster.deal_product_channel on applications.id=deal_product_channel.application_id
    left join ods.ods_los.contacts on contacts.id = applications.loan_officer_id
    where loans.appraisal_paid_at is not null 
      and applications.rejection_status is null 
      and applications.withdrawn_at is null 
      and applications.state in (
          'lead', 'incomplete_app', 'application', 'quote', 'preliminary_review', 
          'initial_underwriting', 'pending_approval', 'initial_terms', 
          'initial_signed_term_sheet', 'final_terms', 'final_signed_term_sheet', 
          'clear_to_close', 'funding_requested', 'funding_reviewed', 
          'funding_submitted', 'funding_sent', 'funding_authorized'
      )
      and property_valuations.appraisal_order_date is null 
      and applications.appraisal_hold = false 
      and (
          property_valuations.promoted_to_appraisal_at is null 
          and property_valuations.promoted_to_internal_valuation_at is null 
      )
) as subquery
where subquery.row_num = 1