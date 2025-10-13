select
    current_timestamp
    ,l.internal_id
    ,l.application_id
    ,p.address
    ,ae.type
    ,ae.created_at as requested_at
    ,concat(ac.code,' ',ac.title) as exception_type
    ,date(l.settlement_date) as closing_date
    ,row_number() over (order by ae.created_at asc) as row_num
from ods.ods_los.application_exceptions ae
    left join ods.ods_los.application_exception_categories ac on ae.category_id = ac.id
    left join ods.ods_los.loans l on ae.application_id = l.application_id 
    left join ods.ods_los.properties p on p.loan_id = l.id
    where (contains(ac.code,'v') or contains(ac.code, 'V'))
    and ac.code not in ('v13','V14')
    and ae.type = 'scenario'
    and ae.status = 'requested'
    and ae.application_id is not null