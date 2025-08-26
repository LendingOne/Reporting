create or replace view ODS.ODS_CLOSING_FORECAST.LOAN_OFFICER_MONTHLY_FORECAST(
	LOAN_OFFICER_NAME,
	CLOSING_CALENDAR_AMOUNT,
	LOAN_OFFICER_ID,
	FORECAST_AMOUNT
) as 

with base_forecast as (
    select *
    from ods.ods_closing_forecast.tableau_query_for_forecast
    where table_type = 'dynamic projections'
),

closing_calendar as (
    select 
        a.loan_officer_id,
        concat(c.first_name, ' ', c.last_name) as loan_officer_name,
        sum(l.total_amount) as closing_calendar_amount
    from ods.ods_los.applications a
    join ods.ods_los.loans l on l.application_id = a.id
    join ods.ods_los.loan_closing_costs lcc on lcc.loan_id = l.id
    join ods.ods_los.contacts c on c.id = a.loan_officer_id
    where a.state in (
        'initial_underwriting', 'pending_approval', 'initial_terms', 
        'initial_signed_term_sheet', 'final_terms', 'final_signed_term_sheet', 
        'clear_to_close', 'funding_requested', 'funding_reviewed', 
        'funding_submitted', 'funding_sent', 'funding_authorized', 
        'issued', 'setup_servicing', 'closed'
    )
      and a.withdrawn_at is null
      and a.rejection_status is null
      and l.appraisal_paid_at is not null
      and l.settlement_date between date_trunc('month', current_date) 
                               and date_trunc('month', current_date) + interval '1 month' - interval '1 day'
      and a.closing_status in ('confirmed','likely','not_confirmed')
    group by a.loan_officer_id, concat(c.first_name, ' ', c.last_name)
),

null_settlement_dates as (
    select
        application_id,
        case 
            when dateadd(day, median_days_to_close, deal_stage_date) >= date_trunc('month', current_date)
             and dateadd(day, median_days_to_close, deal_stage_date) < date_trunc('month', current_date) + interval '1 month'
            then true else false
        end as null_settlement_date
    from base_forecast
),

long_term_deals as (
    select
        application_id,
        case 
            when portfolio = false and datediff(day, deal_stage_date, current_date) >= 45 
            then true else false 
        end as long_term_deals
    from base_forecast
),

date_range as (
    select
        f.application_id,
        case 
            when f.dynamic_settlement_date >= date_trunc('month', current_date)
             and f.dynamic_settlement_date < date_trunc('month', current_date) + interval '1 month'
                then true
            when f.dynamic_settlement_date is null and n.null_settlement_date = true and d.long_term_deals = false
                then true
            else false
        end as date_range
    from base_forecast f
    left join null_settlement_dates n on f.application_id = n.application_id
    left join long_term_deals d on f.application_id = d.application_id
),

closing_status as (
    select
        application_id,
        case 
            when product = 'Bridge' 
                 and closing_status = 'not_confirmed'
                 and datediff(day, closing_status_date, current_date) <= median_days_from_unconfirmed_to_confirmed_bridge
                 and current_date <= dateadd(day, -median_days_from_unconfirmed_to_confirmed_bridge, dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date))))
            then true

            when product = 'Rental' 
                 and closing_status = 'not_confirmed'
                 and datediff(day, closing_status_date, current_date) <= median_days_from_unconfirmed_to_confirmed_rental
                 and current_date <= dateadd(day, -median_days_from_unconfirmed_to_confirmed_rental, dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date))))
            then true

            when current_date < dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date))) 
                 and closing_status in ('confirmed','likely') 
            then true

            when current_date >= dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date))) 
                 and closing_status = 'confirmed'
            then true

            else false
        end as status_flag
    from base_forecast
),

deal_stage_status as (
    select
        application_id,
        case 
            when current_date >= dateadd(day, -5, dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date))))
                 and month(current_date) < month(dateadd(month, 1, date_trunc('day', current_date)))
                 and state in (
                     'clear_to_close', 'closed', 'final_signed_term_sheet', 'final_terms',
                     'funding_authorized', 'funding_requested', 'funding_reviewed',
                     'funding_sent', 'funding_submitted', 'setup_servicing'
                 )
            then true

            when product = 'Bridge'
                 and state in (
                     'application', 'lead', 'incomplete_app', 'quote',
                     'preliminary_review', 'initial_underwriting', 'pending_approval',
                     'initial_terms', 'initial_signed_term_sheet'
                 )
                 and current_date >= dateadd(
                     day,
                     -median_days_to_close_after_final_terms_bridge,
                     dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date)))
                 )
            then false

            when product = 'Rental'
                 and state in (
                     'application', 'lead', 'incomplete_app', 'quote',
                     'preliminary_review', 'initial_underwriting', 'pending_approval',
                     'initial_terms', 'initial_signed_term_sheet'
                 )
                 and current_date >= dateadd(
                     day,
                     -median_days_to_close_after_final_terms_rental,
                     dateadd(day, -1, date_trunc('month', dateadd(month, 1, current_date)))
                 )
            then false

            else true
        end as stage_flag
    from base_forecast
),

forecast_enriched as (
    select distinct
        a.loan_officer_id,
        f.loan_officer_name,
        f.total_amount,
        f.state,
        f.percent_close,
        f.department,
        f.application_id,
        d.date_range,
        c.status_flag,
        ds.stage_flag
    from base_forecast f
    left join date_range d on f.application_id = d.application_id
    left join closing_status c on f.application_id = c.application_id
    left join deal_stage_status ds on f.application_id = ds.application_id
    left join ods.ods_los.applications a on a.id = f.application_id
)

select
    fe.loan_officer_name,
    cc.closing_calendar_amount,
    fe.loan_officer_id,
    sum(
        case 
            when fe.state in (
                'application','lead','incomplete_app','quote',
                'preliminary_review','initial_underwriting','pending_approval'
            )
            then fe.total_amount * 0.10
            else fe.total_amount * fe.percent_close
        end
    ) as forecast_amount
from forecast_enriched fe
left join closing_calendar cc 
    on cc.loan_officer_id = fe.loan_officer_id
where fe.date_range = true
  and fe.status_flag = true
  and fe.stage_flag = true
  and (fe.department is null or fe.department in ('retail', 'tpo'))
  and fe.loan_officer_name not in (
        'Drew Deaton', 'JArouh Correspondent', 'Jordan Hill', 
        'Michael Boggiano', 'Mike Boggiano', 'PCG Jaime Arouh', 
        'PCG Matthew DeTessan', 'Ryan Tamras'
  )
group by fe.loan_officer_name, cc.closing_calendar_amount, fe.loan_officer_id;