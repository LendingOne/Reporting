with date_series_days AS (
  SELECT
    DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2023-01-01') AS date_value
  FROM
    TABLE(GENERATOR(ROWCOUNT => 10000000)) -- Change ROWCOUNT to the desired number of days
),

dateframe as (SELECT date_value::date as day
FROM   date_series_days
left join ods.ods_los.holidays on DATE(date_series_days.date_value)=DATE(ods.ods_los.holidays.holiday_date)
where DAYOFWEEK(date_value) not in (0,6)
AND ods.ods_los.holidays.holiday_date is null
AND date_value <= current_date
), 

main as (SELECT apps.id as application_id
, loans.internal_id as loan_id
, properties.address as property_address
, concat(contacts.first_name,' ',contacts.last_name) as loan_officer_name
, users.department
, deal_product_channel.ops_product as product_type
, apps.appraisal_hold 
, CASE WHEN COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at) is null THEN 'Initial Screening' ELSE apps.state END as deal_status
, loans.appraisal_paid_at as actual_start_date
, CASE WHEN  DAYOFWEEK(loans.appraisal_paid_at)=0 THEN dateadd('day',1,date_trunc('day',loans.appraisal_paid_at))
        WHEN  DAYOFWEEK(loans.appraisal_paid_at)=6 THEN dateadd('day',2,date_trunc('day',loans.appraisal_paid_at))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',loans.appraisal_paid_at)))=0 THEN dateadd('day',2,date_trunc('day',loans.appraisal_paid_at))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',loans.appraisal_paid_at)))=6 THEN dateadd('day',2,date_trunc('day',loans.appraisal_paid_at))
        WHEN hld1.holiday_date is not null THEN dateadd('day',1,date_trunc('day',loans.appraisal_paid_at))
        ELSE loans.appraisal_paid_at END as start_date
, COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at) as actual_end_date
, CASE WHEN  DAYOFWEEK(COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at))=0 THEN Dateadd('day',1,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at)))
        WHEN  DAYOFWEEK(COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at))=6 THEN dateadd('day',2,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at)))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at))))=0 THEN dateadd('day',2,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at)))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at))))=6 THEN dateadd('day',2,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at)))
        WHEN hld2.holiday_date is not null THEN dateadd('day',1,date_trunc('day',COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at)))
        ELSE COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at) END as end_date
from ods.ods_los.applications apps
left join ods.ods_los.loans on apps.id=loans.application_id
inner join ods.ods_los.properties on properties.deleted_at is null 
        and properties.loan_id = loans.id
left join ods.ods_los.property_valuations 
        on property_valuations.property_id = properties.id
left join ods.ods_los.contacts on apps.loan_officer_id=contacts.id
left join ods.ods_los.users on users.contact_id = apps.loan_officer_id
left join ods.ods_sales_roster.deal_product_channel on apps.id=deal_product_channel.application_id
left join ods.ods_los.holidays hld1 on date(loans.appraisal_paid_at)=DATE(hld1.holiday_date)
left join ods.ods_los.holidays hld2 on  DATE(COALESCE(property_valuations.promoted_to_internal_valuation_at,property_valuations.promoted_to_appraisal_at))
    =DATE(hld2.holiday_date)
 where loans.appraisal_paid_at is not null 
      and apps.rejection_status is null 
      and apps.withdrawn_at is null
),

main2 as ( SELECT *
, 23-extract(hour, start_date) as start_hour
    , 60-extract(minute, start_date) as start_minute
    , extract(hour,end_date) as end_hour
    , extract(minute,end_date) as end_minute
from main
), 

main3 as (SELECT application_id, loan_id, property_address, loan_officer_name, department, product_type, appraisal_hold, deal_status, start_date, end_date
, current_timestamp as last_update, actual_start_date, actual_end_date
, CASE WHEN date(start_date)=date(end_date) THEN (min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-24
ELSE (count(dateframe.day)*1440+min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-48 END as kpi_count
from dateframe
left join main2 on date(dateframe.day)>=date(main2.start_date) AND date(dateframe.day)<=date(main2.end_date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),

on_hold_main as (SELECT lg.application_id, old_value, new_value
, changed_at as on_hold_end
, lag(changed_at) over (partition by application_id order by changed_at asc) on_hold_start
from ods.ods_ops_scorecards.tbl_on_hold_que_change_log lg
),

on_hold_main2 as (SELECT application_id, old_value, new_value
, on_hold_start
, CASE WHEN DAYOFWEEK(on_hold_start)=0 THEN dateadd('day',1,date_trunc('day',on_hold_start))
        WHEN  DAYOFWEEK(on_hold_start)=6 THEN dateadd('day',2,date_trunc('day',on_hold_start))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',on_hold_start)))=0 THEN dateadd('day',2,date_trunc('day',on_hold_start))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',on_hold_start)))=6 THEN dateadd('day',2,date_trunc('day',on_hold_start))
        WHEN hld1.holiday_date is not null THEN dateadd('day',1,date_trunc('day',on_hold_start))
        ELSE on_hold_start END as start_date
, on_hold_end
, CASE WHEN  DAYOFWEEK(on_hold_end)=0 THEN Dateadd('day',1,date_trunc('day',on_hold_end))
        WHEN  DAYOFWEEK(on_hold_end)=6 THEN dateadd('day',2,date_trunc('day',on_hold_end))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',on_hold_end)))=0 THEN dateadd('day',2,date_trunc('day',on_hold_end))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',on_hold_end)))=6 THEN dateadd('day',2,date_trunc('day',on_hold_end))
        WHEN hld2.holiday_date is not null THEN dateadd('day',1,date_trunc('day',on_hold_end))
        ELSE on_hold_end END as end_date
from on_hold_main
left join ods.ods_los.holidays hld1 on date(on_hold_start)=DATE(hld1.holiday_date)
left join ods.ods_los.holidays hld2 on  DATE(on_hold_end)=DATE(hld2.holiday_date)
where old_value=TRUE
),

on_hold_main3 as ( SELECT *
, 23-extract(hour, start_date) as start_hour
    , 60-extract(minute, start_date) as start_minute
    , extract(hour,end_date) as end_hour
    , extract(minute,end_date) as end_minute
from on_hold_main2
),

on_hold_main4 as (SELECT application_id, start_date, end_date
, CASE WHEN date(start_date)=date(end_date) THEN (min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-24
ELSE (count(dateframe.day)*1440+min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-48 END as on_hold_hours
from dateframe
left join on_hold_main3 on date(dateframe.day)>=date(on_hold_main3.start_date) AND date(dateframe.day)<=date(on_hold_main3.end_date)
group by 1,2,3
)

SELECT main3.*, on_hold_main4.on_hold_hours
from main3
left join on_hold_main4 on main3.application_id=on_hold_main4.application_id