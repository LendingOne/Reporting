select
    l.application_id
    ,l.total_amount
    ,current_timestamp
    ,p.name
    ,case when l.internal_id is null then l.application_id else l.internal_id end as internal_id
    ,l.settlement_date
    ,concat(c.first_name,' ',c.last_name) as loan_officer_name
    ,t.team
    ,a.state
    ,a.lo_department as los_department
    ,date(l.appraisal_paid_at) as appraisal_paid_date
    ,date(a.submitted_to_initial_underwriting_at) as initial_underwriting_date
    ,a.rejected_at
    ,split_part(a.type, '::', 2) as product
    ,a.long_termed_at
    ,a.withdrawn_at
    ,a.created_at
    ,arr.refunded_at
    ,a.portfolio
    ,a.rejection_status
    ,a.rejection_comment
    ,d.rejection_category
    ,d.rejection_reason
    ,d.rejection_reason_json
    ,d.deal_loss_date
from ods.ods_los.applications a 
    left join ods.ods_los.loans l on a.id = l.application_id 
    left join ods.ods_los.applications_teams t on t.id = a.id
    left join ods.ods_los.contacts c on c.id = a.loan_officer_id
    left join ods.ods_reportcache.deal_losses d on d.application_number = a.id 
    left join ods.ods_los.products p on p.id = a.product_id
    left join ods.ods_los.appraisal_refund_requests arr on arr.application_id = a.id  
where l.appraisal_paid_at is not null