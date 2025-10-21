with matches as (
    select
         c.*
        ,c2.*
        ,case
            when c.entity_name = c2.buyer_borrower1_name then 'entity' -- Match on entity
            when c.email = c2.email1 or c.email = c2.email2 or c.email = c2.email3 then 'email' -- Match on email
            when c.borrower_name = c2.owner_name and c.borrower_state = c2.state then 'name+state' -- Match on name + state
        end as match_type
        ,case
            when c.entity_name = c2.buyer_borrower1_name then 1 -- Priority for entity
            when c.email = c2.email1 or c.email = c2.email2 or c.email = c2.email3 then 2 -- Priority for email
            when c.borrower_name = c2.owner_name and c.borrower_state = c2.state then 3 -- Priority for name + state
        end as match_priority
    from ods.ods_los.all_contacts c
    join ods.ods_reportcache.sfr_list c2
        on c.entity_name = c2.buyer_borrower1_name
        or (c.email = c2.email1 or c.email = c2.email2 or c.email = c2.email3)
        or (c.borrower_name = owner_name and borrower_state = c2.state)
)
select
     *
from matches
order by match_priority, entity_name, buyer_borrower1_name