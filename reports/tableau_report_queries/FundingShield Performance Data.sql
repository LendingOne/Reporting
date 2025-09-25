SELECT concat(ct1.first_name,' ',ct1.last_name) title_reviewer_name
, loans.internal_id as loan_number
, apps.state as closing_status
, apps.type as type
, fnd.updated_at as approved_date
, current_timestamp as last_update
, apps.id as application_id
, loans.settlement_date as closing_date
, lgl.preliminary_commitment_received_date
, prod.ops_product
from ods.ods_los.application_legal_closing_informations lgl
left join ods.ods_los.loans on lgl.application_id=loans.application_id
left join ods.ods_los.funding_shield_transactions fnd on lgl.application_id=fnd.application_id
left join ods.ods_los.applications apps on lgl.application_id=apps.id
left join ods.ods_los.contacts ct1 on apps.TITLE_REVIEWER_ID=ct1.id
left join ods.ods_sales_roster.deal_product_channel prod on lgl.application_id=prod.application_id
where lgl.preliminary_commitment_received_date is not null
and fnd.transaction_status=30