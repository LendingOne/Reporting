with init_terms as (select application_id,
min(created_at) - interval '4 hours' as first_init_terms_dt
from events
where events.state='initial_terms'
group by 1),

fndauth as (select application_id,
max(created_at)- interval '4 hours' as last_fund_auth_dt
from events
where events.state='funding_authorized'
group by 1),

ctc as (select application_id,
max(created_at)- interval '4 hours' as last_ctc_dt
from events
where events.state='clear_to_close'
group by 1),

clsd as (Select application_id
, settlement_date as loan_settlement_date
from loans)

SELECT a.id as application_id
, CASE when a.portfolio=TRUE THEN 'Portfolio' ELSE split_part(d.type,'::',2) END as product
, b.first_init_terms_dt
, c.loan_settlement_date
, e.last_fund_auth_dt
, a.state
, f.last_ctc_dt
, LOCALTIMESTAMP as last_update
from applications a
left join init_terms b on a.id=b.application_id
left join clsd c on a.id=c.application_id
left join products d on a.product_id=d.id
left join fndauth e on a.id=e.application_id
left join ctc f on a.id=f.application_id