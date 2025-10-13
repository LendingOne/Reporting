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

max_term as (SELECT loans.internal_id
, max(tm.created_at) AS last_term_sheet_date
from ods.ods_los.term_sheets tm
left join ods.ods_los.loans on tm.application_id=loans.application_id
where lower(tm.type) like '%initialterms%' and tm.created_at is not null
group by 1
),

min_iuwterm as (SELECT application_id
, min(created_at) initial_uw_date
from ods.ods_los.events
where state in ('initial_underwriting')
group by 1
),

props as (SELECT loans.application_id, count(1) as properties
    from ODS.ODS_LOS.LOANS loans
    left join ODS.ODS_LOS.PROPERTIES props on loans.id=props.loan_id
    WHERE props.deleted_at is null
    group by 1
),

main as (SELECT loans.internal_id as loan_id
, CASE WHEN props.properties>1 THEN 'Portfolio' ELSE split_part(apps.type,'::',2) END as product
, loans.transaction_purpose as loan_type
, deal_product_channel.OPS_CHANNEL as department
, concat(cnt1.first_name,' ',cnt1.last_name) as project_coordinator_name
, concat(cnt2.first_name,' ',cnt2.last_name) as underwriter_name
, concat(cnt3.first_name,' ',cnt3.last_name) as loan_officer_name
, min_iuwterm.initial_uw_date
, max_term.last_term_sheet_date
, CASE WHEN DAYOFWEEK(min_iuwterm.initial_uw_date)=0 THEN dateadd('day',1,date_trunc('day',min_iuwterm.initial_uw_date))
        WHEN  DAYOFWEEK(min_iuwterm.initial_uw_date)=6 THEN dateadd('day',2,date_trunc('day',min_iuwterm.initial_uw_date))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',min_iuwterm.initial_uw_date)))=0 THEN dateadd('day',2,date_trunc('day',min_iuwterm.initial_uw_date))
        WHEN hld1.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',min_iuwterm.initial_uw_date)))=6 THEN dateadd('day',2,date_trunc('day',min_iuwterm.initial_uw_date))
        WHEN hld1.holiday_date is not null THEN dateadd('day',1,date_trunc('day',min_iuwterm.initial_uw_date))
        ELSE min_iuwterm.initial_uw_date END as start_date
, CASE WHEN  DAYOFWEEK(max_term.last_term_sheet_date)=0 THEN Dateadd('day',1,date_trunc('day',max_term.last_term_sheet_date))
        WHEN  DAYOFWEEK(max_term.last_term_sheet_date)=6 THEN dateadd('day',2,date_trunc('day',max_term.last_term_sheet_date))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',max_term.last_term_sheet_date)))=0 THEN dateadd('day',2,date_trunc('day',max_term.last_term_sheet_date))
        WHEN hld2.holiday_date is not null AND DAYOFWEEK(dateadd('day',1,date_trunc('day',max_term.last_term_sheet_date)))=6 THEN dateadd('day',2,date_trunc('day',max_term.last_term_sheet_date))
        WHEN hld2.holiday_date is not null THEN dateadd('day',1,date_trunc('day',max_term.last_term_sheet_date))
        ELSE max_term.last_term_sheet_date END as end_date        
from ods.ods_los.applications apps
left join ods.ods_los.loans on apps.id=loans.application_id
inner join max_term on loans.internal_id=max_term.internal_id
left join ods.ods_sales_roster.deal_product_channel on apps.id=deal_product_channel.application_id
left join min_iuwterm on min_iuwterm.application_id=apps.id
left join ods.ods_los.holidays hld1 on date(initial_uw_date)=DATE(hld1.holiday_date)
left join ods.ods_los.holidays hld2 on DATE(max_term.last_term_sheet_date)=DATE(hld2.holiday_date)
left join ods.ods_los.contacts cnt1 on apps.project_coordinator_id=cnt1.id
left join ods.ods_los.contacts cnt2 on apps.underwriter_id=cnt2.id
left join ods.ods_los.contacts cnt3 on apps.loan_officer_id=cnt3.id
left join props on props.application_id=apps.id
where apps.rejected_at is null

),

main2 as ( SELECT *
, 23-extract(hour, start_date) as start_hour
    , 60-extract(minute, start_date) as start_minute
    , extract(hour,end_date) as end_hour
    , extract(minute,end_date) as end_minute
from main
) 

SELECT main2.*, current_timestamp as last_update
, CASE WHEN date(start_date)=date(end_date) THEN (min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-24
ELSE (count(dateframe.day)*1440+min(start_hour)*60+min(start_minute)+min(end_hour)*60+min(end_minute))/60-48 END as kpi_count
from dateframe
left join main2 on date(dateframe.day)>=date(main2.start_date) AND date(dateframe.day)<=date(main2.end_date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15