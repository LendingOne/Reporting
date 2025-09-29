select
concat(c.first_name, ' ',c.last_name) as project_coordinator_name
,c.id
,date(l.settlement_date) as settlement_date
,l.internal_id
from ods.ods_los.loans l 
join ods.ods_los.applications a on a.id = l.application_id 
join ods.ods_los.contacts c on c.id = a.project_coordinator_id
left join ods.ods_ops_scorecards.team_assignments t on t.TEAM_MEMBER_NAME = concat(c.first_name, ' ',c.last_name)
left join ods.ods_sales_roster.deal_product_channel d on d.internal_id = l.internal_id
where a.state in ('funding_requested','funding_authorized','funding_reviewed','closed','funding_sent','clear_to_close','funding_submitted','setup_servicing')
and a.closing_status = 'confirmed'
and  (t.team = 'PC')