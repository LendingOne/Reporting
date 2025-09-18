select
    l.internal_id as loan_number
    ,ae.id
    ,current_timestamp as current_time
    ,a.state
    ,date(l.settlement_date) as settlement_date
    ,a.id as application_id
    ,ae.status
    ,concat(c.first_name, ' ',c.last_name) as Exception_Author
    ,u.role
    ,ae.created_at
    ,split_part(a.type, ':',3) as product
    ,case when l.properties_count  > 1 THEN 'Portfolio' ELSE 'Single Asset' END as loan_type
    ,case when a.lo_department is null then 'retail' else a.lo_department end as sales_department
    ,p.name as product_type
    ,ae.description
    ,l.properties_count
    ,ae.level_id
    ,current_date
    ,ac.title
    ,ae.compensating_factors
    ,ae.compensating_factor_tags
    ,a.lo_department as department
    ,ac.exception_type
    ,case 
        when el.priority is null then ''
        when el.priority = 2147483647 then 'Pricing'
        when el.priority = 2147483646 then 'Underwriting'
        when el.priority = -1 then el.name -- assuming name is in application_exception_levels
        else cast(el.priority as varchar)
    end as display_priority
from ods.ods_los.applications a
    left join ods.ods_los.application_exceptions ae on ae.application_id = a.id
    left join ods.ods_los.application_exception_categories ac on ac.id = ae.category_id
    left join ods.ods_los.application_exception_levels el on ac.level_id = el.id
    left join ods.ods_los.loans l on a.id = l.application_id
    left join ods.ods_los.products p on p.id = a.product_id
    left join ods.ods_los.contacts c on ae.author_id = c.id
    left join ods.ods_los.users u on u.contact_id = c.id
    where a.state in ('setup_servicing','closed')