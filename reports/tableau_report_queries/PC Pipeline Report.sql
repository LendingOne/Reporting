with date_series_days AS (
  SELECT
    DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2023-01-01') AS date_value
  FROM
    TABLE(GENERATOR(ROWCOUNT => 100000)) -- Change ROWCOUNT to the desired number of days
),
    dateframe as (SELECT date_value::date as dateframe
    FROM   date_series_days
    left join ods.ods_los.holidays on DATE(date_series_days.date_value)=DATE(ods.ods_los.holidays.holiday_date)
    and ods.ods_los.holidays.name in ('New Year''s Day','Martin Luther King Day','Memorial Day','Juneteenth National Independence Day','Independence Day','Labor Day','Thanksgiving','Day after Thanksgiving (Black Friday)','Christmas Eve','Christmas Day')
     left join ods.ods_los.bank_holidays_not_in_holidays_table h on date(date_series_days.date_value) = date(h.holiday_date)
    and h.holiday_name in ('Veterans Day','Columbus Day')
    where DAYOFWEEK(date_value) not in (0,6)
    AND ods.ods_los.holidays.holiday_date is null
),


dateframe2 as (SELECT dateframe
, LAG(dateframe, 1) OVER (ORDER BY dateframe asc) as neg_1
, LAG(dateframe, 2) OVER (ORDER BY dateframe asc) as neg_2
, LAG(dateframe, 3) OVER (ORDER BY dateframe asc) as neg_3
, LAG(dateframe, 4) OVER (ORDER BY dateframe asc) as neg_4
, LAG(dateframe, 5) OVER (ORDER BY dateframe asc) as neg_5
, LAG(dateframe, 6) OVER (ORDER BY dateframe asc) as neg_6
, LAG(dateframe, 9) OVER (ORDER BY dateframe asc) as neg_9
, LAG(dateframe, 15) OVER (ORDER BY dateframe asc) as neg_15
, LAG(dateframe, 20) OVER (ORDER BY dateframe asc) as neg_20
, LAG(dateframe, 25) OVER (ORDER BY dateframe asc) as neg_25
, LAG(dateframe, 30) OVER (ORDER BY dateframe asc) as neg_30
, LAG(dateframe, 35) OVER (ORDER BY dateframe asc) as neg_35
, LAG(dateframe, 50) OVER (ORDER BY dateframe asc) as neg_50
, LEAD(dateframe, 5) OVER (ORDER BY dateframe asc) as pos_5
, LEAD(dateframe, 7) OVER (ORDER BY dateframe asc) as pos_7
, LEAD(dateframe, 1) OVER (ORDER BY dateframe asc) as pos_1
, LEAD(dateframe, 2) OVER (ORDER BY dateframe asc) as pos_2
, LEAD(dateframe, 10) OVER (ORDER BY dateframe asc) as pos_10
from dateframe
),


dateframe3 as (SELECT DISTINCT dateframe2.*, apps.id as application_id
from dateframe2
left join ods.ods_los.applications apps
where dateframe=current_date

),


title as (
    select 
        f.loan_id
        ,f.date
        ,f.via
        ,row_number() over(partition by f.loan_id order by f.date desc) as row_num
    from ods.ods_los.loan_follow_ups f
    left join ods.ods_los.loans l on l.id = f.loan_id
    where type = 'title'

),



latest_follow_up_title as(

    select
    *
    from title t
    where t.row_num = 1 
    
),

borrower as (
    select 
        f.loan_id
        ,f.date
        ,f.via 
        ,row_number() over(partition by f.loan_id order by f.date desc) as row_num
    from ods.ods_los.loan_follow_ups f
    left join ods.ods_los.loans l on l.id = f.loan_id
    where type = 'borrower'
),

latest_follow_up_borrower as(

    select
    *
    from borrower b
    where b.row_num = 1 
    
),

initial_terms_date as (

    select
    a.id as application_number
    ,min(date(ts.created_at)) as initial_terms_date
    from ods.ods_los.applications a 
    left join ods.ods_los.term_sheets ts on ts.application_id = a.id 
    left join ods.ods_los.loans l on l.application_id = a.id
    where ts.type = 'TermSheet::InitialTerms'
    group by a.id 
),

prelim_review_table as (
  select
    a.id
    ,ds.preliminary_review_date
  from ods.ods_los.applications a
  left join ods.ods_closing_forecast.deal_stage_dates ds on ds.application_id = a.id
  left join ods.ods_sales_roster.deal_product_channel dc on dc.application_id = a.id
  where a.state = 'preliminary_review'
  and dc.ops_channel != 'Private Client Group'
  and a.rejected_at is null and a.withdrawn_at is null and a.long_termed_at is null 
),

prelim_review_view as (
select
    p.id 
    ,ds.preliminary_review_date
from prelim_review_table p
left join ods.ods_closing_forecast.deal_stage_time_stamps ds on ds.application_id = p.id 
where p.preliminary_review_date is null 
),

preliminary_review_date as (
select
    p.id
    ,coalesce(p.preliminary_review_date, p2.preliminary_review_date) as preliminary_review_date
from prelim_review_table p
left join prelim_review_view p2 on p2.id = p.id),


  preliminary_review_welcome_call as(select
    a.id
    ,case 
        when (a.state = 'initial_underwriting' and date(a.submitted_to_initial_underwriting_at) = date(current_date)) or (a.state = 'preliminary_review' and date(d.preliminary_review_date) = date(current_date)) then 'Due Tomorrow'
        when (a.state = 'initial_underwriting' and date(a.submitted_to_initial_underwriting_at) = dt.neg_1) or (a.state = 'preliminary_review' and date(d.preliminary_review_date) = dt.neg_1) then 'Due Today' 
        when (a.state = 'initial_underwriting' and date(a.submitted_to_initial_underwriting_at) = dt.neg_2 and al.kickoff_call_with_borrower_date is null) or (a.state = 'preliminary_review' and                       date(d.preliminary_review_date) <= dt.neg_2) then 'Past Due' end as preliminary_review_welcome_call_kpi
    from ods.ods_los.applications a 
    left join preliminary_review_date d on d.id = a.id
    left join dateframe3 dt on a.id=dt.application_id
    left join ods.ods_los.APPLICATION_LEGAL_CLOSING_INFORMATIONS al on al.application_id = a.id
    where a.state in ('initial_underwriting','preliminary_review')
    and a.withdrawn_at is null and a.rejected_at is null and a.long_termed_at is null and al.kickoff_call_with_borrower_date is null),

preliminary_review_follow_up_call_1 as (
  select
    a.id
    ,case 
      when date(d.preliminary_review_date) = dt.neg_2  and al.kickoff_call_with_borrower_date is not null and a.submitted_to_initial_underwriting_at is null then 'Due Tomorrow'
      when date(d.preliminary_review_date) = dt.neg_3 and al.kickoff_call_with_borrower_date is not null and a.submitted_to_initial_underwriting_at is null then 'Due Today'
      when date(d.preliminary_review_date) = dt.neg_4 and al.kickoff_call_with_borrower_date is not null and a.submitted_to_initial_underwriting_at is null then 'Past Due'
    end as preliminary_review_follow_up_call_1_kpi
  from ods.ods_los.applications a
  left join preliminary_review_date d on d.id = a.id
  left join dateframe3 dt on a.id = dt.application_id
  left join ods.ods_los.application_legal_closing_informations al on al.application_id = a.id
  where a.state = 'preliminary_review'
),

preliminary_review_follow_up_call_2 as (
    select
        a.id
        ,case 
            when (a.state = 'preliminary_review' 
                  and date(d.preliminary_review_date) = dt.neg_4 
                  and al.kickoff_call_with_borrower_date is not null 
                  and a.submitted_to_initial_underwriting_at is null 
                  and (prfu1.preliminary_review_follow_up_call_1_kpi is null or prfu1.preliminary_review_follow_up_call_1_kpi != 'Past Due')) 
                then 'Due Tomorrow'
            when (a.state = 'preliminary_review' 
                  and date(d.preliminary_review_date) = dt.neg_5 
                  and al.kickoff_call_with_borrower_date is not null 
                  and a.submitted_to_initial_underwriting_at is null) 
                then 'Due Today'
            when (a.state = 'preliminary_review' 
                  and date(d.preliminary_review_date) = dt.neg_6 
                  and al.kickoff_call_with_borrower_date is not null 
                  and a.submitted_to_initial_underwriting_at is null) 
                then 'Past Due'
        end as preliminary_review_follow_up_call_2_kpi
    from ods.ods_los.applications a 
    left join preliminary_review_date d on d.id = a.id
    left join dateframe3 dt on a.id = dt.application_id
    left join ods.ods_los.application_legal_closing_informations al on al.application_id = a.id
    left join preliminary_review_follow_up_call_1 prfu1 on prfu1.id = a.id
    where a.state = 'preliminary_review'
),




   closing_kpi as ( select
    a.id as application_number
    ,case  
        when (a.closing_status is null or a.closing_status in ('likely','not_confirmed'))  and (date(l.settlement_date) = dt.pos_2) then '2 Business Days'
        when (a.closing_status is null or a.closing_status in ('likely','not_confirmed'))  and (date(l.settlement_date) = dt.pos_1) then 'Next Business Day' 
        when (a.closing_status is null or a.closing_status in ('likely','not_confirmed'))  and date(l.settlement_date) = current_date then 'Today'
        when (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) and date(l.settlement_date) < current_date then 'Past Due'
        when (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) and date(l.settlement_date) is null then 'No Settlement Date'
        when (a.closing_status = 'not_confirmed' and l.settlement_date is not null) then 'Not Likely'
        else 'Future'
        end as closing_kpi
    from ods.ods_los.applications a 
        left join ods.ods_los.loans l on a.id = l.application_id
        left join dateframe3 dt on a.id=dt.application_id
    where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
    and a.submitted_to_initial_underwriting_at is not null),



borrower_follow_up_kpi as (

     select
     a.id as application_number
     ,case 
        when (date(bf.date) <= dt.neg_3
             and date(l.settlement_date)  <= dt.pos_10 
             and l.transaction_purpose in ('purchase', 'refinance'))
             or (date(bf.date) <= dt.neg_4
             and l.settlement_date > dt.pos_10  --update 7 to 10
             and l.transaction_purpose in ('purchase', 'refinance')) 
             or (date(bf.date) < dt.neg_5 and l.settlement_date is null) then 'Past Due'
       when (date(bf.date) = dt.neg_2
             and l.settlement_date <= dt.pos_10 
             and l.transaction_purpose in ('purchase','refinance'))
             or (date(bf.date) = dt.neg_3
             and l.settlement_date > dt.pos_10
             and l.transaction_purpose in ('purchase', 'refinance')) 
             or (date(bf.date) = dt.neg_5 and l.settlement_date is null) then 'Due Today'
       when (date(bf.date) = dt.neg_1 
             and l.settlement_date <= dt.pos_10 
             and l.transaction_purpose in ('purchase', 'refinance'))
             or (date(bf.date) = dt.neg_2
             and l.settlement_date > dt.pos_10
             and l.transaction_purpose in ('purchase', 'refinance')) 
             or (date(bf.date) = dt.neg_4 and l.settlement_date is null) then 'Due Tomorrow'
    end as follow_up_kpi
    from ods.ods_los.applications a 
            left join ods.ods_los.contacts c on a.project_coordinator_id = c.id 
            left join ods.ods_los.loans l on a.id = l.application_id
            left join latest_follow_up_borrower bf on bf.loan_id = l.id
            left join initial_terms_date ts on ts.application_number = a.id 
            left join dateframe3 dt on a.id=dt.application_id     
            where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close','initial_underwriting')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) 
            and  ts.initial_terms_date is not null 
),


initial_terms_kpi as (

    select
    a.id as application_number
        ,case 
        when (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and ts.initial_terms_date <= dt.neg_3) 
             or (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_7 and date(ts.initial_terms_date) = dt.neg_2) then 'Past Due'
        when (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and ts.initial_terms_date <= dt.neg_2) 
             or (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_7 and date(ts.initial_terms_date) = dt.neg_1) then  'Due Today'
        when (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and ts.initial_terms_date <= dt.neg_1) 
             or (a.state in ('initial_terms','initial_signed_term_sheet','final_terms','final_signed_term_sheet') and date(a2.term_sheet_reviewed_with_borrower_date) is null and date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_7 and ts.initial_terms_date = current_date) then 'Due Tomorrow'
        end as initial_terms_kpi   
    from ods.ods_los.applications a 
            left join ods.ods_los.loans l on a.id = l.application_id
            left join latest_follow_up_borrower bf on bf.loan_id = l.id
            left join initial_terms_date ts on ts.application_number = a.id 
            left join dateframe3 dt on a.id=dt.application_id
            left join ods.ods_los.application_legal_closing_informations a2 on a2.application_id = a.id 
        where l.transaction_purpose in ('purchase','refinance')
            and a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close','initial_underwriting')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) 
            and ts.initial_terms_date is not null 
),

title_order as (

    select 
    a.id as application_number
    ,case  
        when datediff('hour',a.submitted_to_initial_underwriting_at, current_timestamp) >= 24
         and (date(le.title_commitment_order_date) is null and date(le.title_commitment_most_recent_order_date) is null) then 
        'Outstanding'
    end as Title_Order
    from ods.ods_los.applications a 
    left join ods.ods_los.application_legal_closing_informations le on le.application_id = a.id     
    where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
    and a.submitted_to_initial_underwriting_at is not null 
    ),


 title_order_confirmation as (
    select 
    a.id as application_number
     ,case   
        when (date(le.title_commitment_order_date) is not null or date(le.title_commitment_most_recent_order_date) is not null) and tf.date is null 
             and ((current_date > date(le.title_commitment_order_date))
                  or (current_date > date(le.title_commitment_most_recent_order_date))) then 
            'Outstanding' 
        end as Title_Order_Confirmation
        from ods.ods_los.applications a 
    left join ods.ods_los.application_legal_closing_informations le on le.application_id = a.id 
    left join ods.ods_los.loans l on a.id = l.application_id
    left join latest_follow_up_title tf on tf.loan_id = l.id 
    where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
    and a.submitted_to_initial_underwriting_at is not null ),


title_follow_up as (   
    select 
     a.id as application_number
     ,case 
        when date(tf.date) < dt.neg_5  and le.preliminary_commitment_received_date is null
             or (date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_5
                 and date(le.preliminary_commitment_received_date) is null 
                 and date(tf.date) <= dt.neg_2) then 
            'Past Due'
        when date(tf.date) = dt.neg_5 and le.preliminary_commitment_received_date is null
             or (date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_5
                 and date(le.preliminary_commitment_received_date) is null 
                 and date(tf.date) = dt.neg_1) then 
            'Due Today'
        when date(tf.date) = dt.neg_4  and le.preliminary_commitment_received_date is null
             or (date(l.settlement_date) >= current_date and date(l.settlement_date) <= dt.pos_5
                 and date(le.preliminary_commitment_received_date) is null 
                 and date(tf.date) = current_date) then 
            'Due Tomorrow'
        else 
            'Future'
    end as title_follow_up
    from ods.ods_los.applications a 
    left join ods.ods_los.application_legal_closing_informations le on le.application_id = a.id 
    left join ods.ods_los.loans l on a.id = l.application_id
    left join initial_terms_date ts on ts.application_number = a.id 
    left join latest_follow_up_title tf on tf.loan_id = l.id 
    left join dateframe3 dt on a.id=dt.application_id
    where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
    and ts.initial_terms_date is not null 

),


appraisal_missing_kpi as (
     select
        a.id as application_number
       ,case when date(a.submitted_to_initial_underwriting_at) <= dt.neg_20 and date(pv.appraisal_approved_date) is null then '>20 days'
        when date(a.submitted_to_initial_underwriting_at) <= dt.neg_15 and date(pv.appraisal_approved_date) is null then '>15 days'
        when date(a.submitted_to_initial_underwriting_at) <= dt.neg_9 and date(pv.appraisal_approved_date) is null then '>9 days'
        end as appraisal_kpi
    from ods.ods_los.applications a
    left join ods.ods_los.loans l on a.id = l.application_id
    left join ods.ods_los.properties p on p.loan_id = l.id 
    left join ods.ods_los.property_valuations pv on pv.property_id = p.id
    left join dateframe3 dt on a.id=dt.application_id
     where a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
      and (a.closing_status is null or a.closing_status in ('likely','not_confirmed')
      and a.portfolio = false)
      and a.submitted_to_initial_underwriting_at is not null 
),


Processing_times_portfolio as (
select
    a.id as application_number
    ,case 
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_50 then '>50 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_35 then  '>35 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_25 then '>25 Days'
        end as processing_times_portfolio
    from ods.ods_los.applications a
    left join ods.ods_los.products p on p.id = a.product_id 
    left join dateframe3 dt on a.id=dt.application_id
    left join ods.ods_los.loans l on l.application_id = a.id 
    where (a.portfolio = true or l.properties_count > 1)
    and a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed'))
    and a.submitted_to_initial_underwriting_at is not null 
),

Processing_times_new_construction as (
select
    a.id as application_number
        ,case 
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_35 then '>35 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_30 then '>30 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_25  then '>25 Days'
        end as processing_times_new_construction
    from ods.ods_los.applications a
    left join dateframe3 dt on a.id=dt.application_id
   left join ods.ods_los.products p on p.id = a.product_id 
   left join ods.ods_los.loans l on l.application_id = a.id
    where p.name = 'New Construction'
    and (a.portfolio = false or l.properties_count = 1)
    and a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) 
            and a.submitted_to_initial_underwriting_at is not null

),


Processing_times_kpi_rental as (   

    select
    a.id as application_number
    ,case 
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_50 then '>50 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_35 then  '>35 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_25 then '>25 Days'
        end as processing_times_rental
    from ods.ods_los.applications a
    left join ods.ods_los.products p on p.id = a.product_id 
    left join ods.ods_los.loans l on l.application_id = a.id
    left join dateframe3 dt on a.id=dt.application_id
    where (a.portfolio = false or l.properties_count = 1)  
    and (a.type = 'Application::Rental' and p.name != 'New Construction')
    and a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed'))
    and a.submitted_to_initial_underwriting_at is not null 

           

),

Processing_times_kpi_bridge as (   
    select
    a.id as application_number
        ,case 
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_30 then '>30 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_25  then '>25 Days'
        when date(a.submitted_to_initial_underwriting_at) < dt.neg_20 then '>20 Days'
        end as progressing_times_bridge
    from ods.ods_los.applications a
    left join dateframe3 dt on a.id=dt.application_id
   left join ods.ods_los.loans l on l.application_id = a.id
   left join ods.ods_los.products p on p.id = a.product_id
    where (a.portfolio = false or l.properties_count = 1)       
    and (a.type = 'Application::Bridge'  and p.name != 'New Construction')
    and a.state not in ('application','quote','incomplete_app','prelminary_review','lead','setup_servicing','closed','clear_to_close')
            and (a.closing_status is null or a.closing_status in ('likely','not_confirmed')) 
            and a.portfolio = false
            and a.submitted_to_initial_underwriting_at is not null

),

cda_kpi as (

    select
    a.id as application_number 
    ,current_date
    ,v.appraisal_received_date
    ,case 
        when date(v.appraisal_received_date) <= dt.neg_2  
             and (v.cda_order_date is null and v.cda_received_date is null) then 
            'Past Due' 
    end as cda_kpi    
    from ods.ods_los.applications a 
    left join ods.ods_los.loans l on a.id = l.application_id
    left join ods.ods_los.properties p on p.loan_id = l.id
    left join ods.ods_los.property_valuations v on v.property_id = p.id
    left join dateframe3 dt on a.id=dt.application_id
    where a.portfolio = false
    and a.type = 'Application::Rental'
    and a.submitted_to_initial_underwriting_at is not null 
    

),
  

kpi as (select
    concat(c.first_name, ' ',c.last_name) as project_coordinator
    ,l.internal_id as loan_number
    ,l.appraisal_paid_at
    ,al.kickoff_call_with_borrower_date
    ,a.id as application_number
    ,a.created_at
    ,a.closing_status
    ,a.type
    ,a.in_conditions_review_from
    ,a.requested_for_ctc
    ,current_timestamp
    ,l.application_id
    ,date(bf.date) as borrower_follow_up_date
    ,upper(left(bf.via, 1)) || lower(substr(bf.via, 2)) as borrower_follow_up_type
    ,date(tf.date) as title_follow_up_date
    ,upper(left(tf.via, 1)) || lower(substr(tf.via, 2))  as title_follow_up_type
    ,a.state
    ,date(l.settlement_date) as closing_date
    ,ck.closing_kpi
    ,bk.follow_up_kpi
    ,l.transaction_purpose
    ,ik.initial_terms_kpi
    ,tr.Title_Order
    ,tc.Title_Order_Confirmation
    ,tfu.title_follow_up
    ,ak.appraisal_kpi
    ,pk.processing_times_rental
    ,pbk.progressing_times_bridge
    ,nc.processing_times_new_construction
    ,pf.processing_times_portfolio
    ,cdk.cda_kpi 
    ,prc.preliminary_review_welcome_call_kpi
    ,prc2.preliminary_review_follow_up_call_1_kpi
    ,prc3.preliminary_review_follow_up_call_2_kpi
    ,concat(c2.first_name, ' ',c2.last_name) as borrower_name
    ,row_number() over(partition by a.id order by a.created_at desc) as row_num
from ods.ods_los.applications a 
    left join ods.ods_los.contacts c on a.project_coordinator_id = c.id 
    left join ods.ods_los.loans l on a.id = l.application_id
    left join latest_follow_up_title tf on tf.loan_id = l.id 
    left join latest_follow_up_borrower bf on bf.loan_id = l.id
    left join closing_kpi ck on ck.application_number = a.id 
    left join borrower_follow_up_kpi bk on bk.application_number = a.id 
    left join initial_terms_kpi ik on ik.application_number = a.id 
    left join title_follow_up tfu on tfu.application_number = a.id 
    left join title_order tr on tr.application_number = a.id 
    left join title_order_confirmation tc on tc.application_number = a.id 
    left join appraisal_missing_kpi ak on ak.application_number = a.id
    left join Processing_times_kpi_rental pk on pk.application_number = a.id 
    left join Processing_times_kpi_bridge pbk on pbk.application_number = a.id 
    left join cda_kpi cdk on cdk.application_number = a.id 
    left join ods.ods_los.appraisal_refund_requests arr on arr.application_id = a.id   
    left join ods.ods_los.users u on u.contact_id = a.loan_officer_id
    left join ods.ods_los.borrowers b on b.application_id = a.id 
    left join ods.ods_los.application_legal_closing_informations al on al.application_id = a.id 
    left join initial_terms_date td on td.application_number = a.id
    left join Processing_times_new_construction nc on nc.application_number = a.id
    left join Processing_times_portfolio pf on pf.application_number = a.id 
    left join ods.ods_los.contacts c2 on c2.id = b.contact_id
    left join preliminary_review_welcome_call prc on prc.id = a.id 
    left join preliminary_review_follow_up_call_1 prc2 on prc2.id = a.id 
    left join preliminary_review_follow_up_call_2 prc3 on prc3.id = a.id 
    left join ods.ods_ops_scorecards.team_assignments t on t.TEAM_MEMBER_NAME = concat(c.first_name, ' ',c.last_name)
where a.rejected_at is null 
    and a.type in ('Application::Bridge','Application::Rental')
    and a.withdrawn_at is null
    and (t.team = 'PC')
    
    and a.project_coordinator_id is not null 
    and a.state not in ('application','quote','issued','incomplete_app','preliminary_review','lead','setup_servicing','closed'))





    

     select
     *
     from kpi
     where kpi.row_num = 1