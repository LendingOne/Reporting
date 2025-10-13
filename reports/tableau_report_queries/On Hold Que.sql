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

hold_notes as (SELECT target_id as application_id
, body as hold_reason
, created_at
, DATEADD(minute, -30,created_at) as on_hold_start
, DATEADD(minute, 30,created_at) as on_hold_end
from ods.ods_los.notes
where body like '%The loan submission has been placed on%'
and lower(target_type)='application'

),

main as (SELECT lg.application_id
, changed_at as on_hold_start
, hold_notes.hold_reason
, loans.appraisal_paid_at
, loans.internal_id as loan_id
from ods.ods_ops_scorecards.tbl_on_hold_que_change_log lg
left join hold_notes on changed_at>=(on_hold_start) AND changed_at<=(on_hold_end) AND lg.application_id=hold_notes.application_id
left join ods.ods_los.loans on lg.application_id=loans.application_id
where lg.old_value=FALSE
--and date(changed_at)>='2025-07-01'
),

add_info as (SELECT b.application_id, count(a.day) as workdays_since_appraisal_paid
from dateframe a
left join main b on date(a.day)>=date(b.appraisal_paid_at) AND DATE(a.day)<=CURRENT_DATE
where b.application_id is not null
group by 1
)

SELECT b.*, c.workdays_since_appraisal_paid, count(a.day) as workdays_on_hold
, row_number() over (order by on_hold_start asc) as row_num
from dateframe a
left join main b on date(a.day)>=date(b.on_hold_start) AND DATE(a.day)<=CURRENT_DATE
left join add_info c on b.application_id=c.application_id
left join ods.ods_los.applications apps on b.application_id=apps.id
where b.application_id is not null
and apps.appraisal_hold=TRUE
and DATE(b.on_hold_start)>='2025-07-01'
group by 1,2,3,4,5,6