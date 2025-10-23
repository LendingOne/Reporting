-- Original data for January 2025 plus summary rows



WITH checkquery AS (
    WITH summary AS (
        SELECT 
            CS_KPI,
            -- CS_LOWER_BOUND,
            CS_MIN_TARGET__DE,
            CS_MIN_TARGET__ST,
            -- CS_PERCENT_OF_TARGET,
            -- CS_PK,
            CS_SCORE,
            CS_TARGET,
            -- CS_UPPER_BOUND,
            CS_WEIGHT,
            CS_WEIGHTED_SCORE,
            DATEFRAME,
            -- FR_LOAN_ADVISOR_NAME,
            FR_QUESTION,
            FR_RESPONSE,
            FR_SCORE,
            FR_SCORED_BY,
            FR_SCORE_DATE,
            FR_TIMESTAMP,
            -- FR_TITLE,
            KPI,
            KPI_JOIN,
            -- KT_KPI,
            KT_METRIC_TARGET,
            -- KT_MINIMUM_TARGET__DE,
            -- KT_MINIMUM_TARGET__ST,
            -- KT_PK,
            KT_TARGET,
            LOAN_OFFICER_NAME,
            ORIGINATION_TARGET,
            ORIGINATION_TARGET_JOIN,
            -- OT_CONCAT,
            -- OT_EMPLOYEE,
            OT_MONTH,
            OT_TARGET,
            ROLE,
            -- ROSTER_CONTACT_ID,
            -- ROSTER_CONTACT_ID__ST,
            -- ROSTER_END_DATE,
            -- ROSTER_END_DATE__TI,
            -- ROSTER_LOAN_OFFICER_NAME,
            -- ROSTER_START_DATE,
            ROSTER_TEAM,
            SALES_KPI,
            VALUE,
            CASE 
    WHEN sales_kpi = 'Pipeline' THEN value / (
CASE 
    WHEN TRY_CAST(REPLACE(REPLACE(ot_target, '$', ''), ',', '') AS DECIMAL(18,2)) = 0 THEN 1
    ELSE TRY_CAST(REPLACE(REPLACE(ot_target, '$', ''), ',', '') AS DECIMAL(18,2))
END
        ) 
END AS actual_origination_pipeline

FROM ODS.ODS_RETAIL_SCORECARD_BUILDER.ROW_LEVEL_RETAIL_SCORECARD

        
        UNION ALL
        -- Objective Score
        SELECT 
            'Summary' AS CS_KPI,
            -- NULL AS CS_LOWER_BOUND,
            NULL AS CS_MIN_TARGET__DE,
            NULL AS CS_MIN_TARGET__ST,
            -- NULL AS CS_PERCENT_OF_TARGET,
            -- NULL AS CS_PK,
            NULL AS CS_SCORE,
            NULL AS CS_TARGET,
            -- NULL AS CS_UPPER_BOUND,
            NULL AS CS_WEIGHT,
            SUM(CS_WEIGHTED_SCORE) AS CS_WEIGHTED_SCORE,
            DATEFRAME,
            -- NULL AS FR_LOAN_ADVISOR_NAME,
            NULL AS FR_QUESTION,
            NULL AS FR_RESPONSE,
            SUM(CS_WEIGHTED_SCORE) AS FR_SCORE,
            NULL AS FR_SCORED_BY,
            NULL AS FR_SCORE_DATE,
            NULL AS FR_TIMESTAMP,
            -- NULL AS FR_TITLE,
            'Summary' AS KPI,
            NULL AS KPI_JOIN,
            -- NULL AS KT_KPI,
            NULL AS KT_METRIC_TARGET,
            -- NULL AS KT_MINIMUM_TARGET__DE,
            -- NULL AS KT_MINIMUM_TARGET__ST,
            -- NULL AS KT_PK,
            NULL AS KT_TARGET,
            LOAN_OFFICER_NAME,
            NULL AS ORIGINATION_TARGET,
            NULL AS ORIGINATION_TARGET_JOIN,
            -- NULL AS OT_CONCAT,
            -- NULL AS OT_EMPLOYEE,
            NULL AS OT_MONTH,
            NULL AS OT_TARGET,
            NULL AS ROLE,
            -- NULL AS ROSTER_CONTACT_ID,
            -- NULL AS ROSTER_CONTACT_ID__ST,
            -- NULL AS ROSTER_END_DATE,
            -- NULL AS ROSTER_END_DATE__TI,
            -- NULL AS ROSTER_LOAN_OFFICER_NAME,
            -- NULL AS ROSTER_START_DATE,
            NULL AS ROSTER_TEAM,
            'Summary' AS SALES_KPI,
            NULL AS VALUE,
            null as actual_origination_pipeline
        FROM ODS.ODS_RETAIL_SCORECARD_BUILDER.ROW_LEVEL_RETAIL_SCORECARD
        GROUP BY DATEFRAME, LOAN_OFFICER_NAME
        
        UNION ALL
        -- Subjective Score
        SELECT 
            'Subjective Score' AS CS_KPI,
            -- NULL AS CS_LOWER_BOUND,
            NULL AS CS_MIN_TARGET__DE,
            NULL AS CS_MIN_TARGET__ST,
            -- NULL AS CS_PERCENT_OF_TARGET,
            -- NULL AS CS_PK,
            NULL AS CS_SCORE,
            NULL AS CS_TARGET,
            -- NULL AS CS_UPPER_BOUND,
            NULL AS CS_WEIGHT,
            SUM(FR_SCORE)/4 AS CS_WEIGHTED_SCORE,
            DATEFRAME,
            -- NULL AS FR_LOAN_ADVISOR_NAME,
            NULL AS FR_QUESTION,
            NULL AS FR_RESPONSE,
            SUM(FR_SCORE)/4 AS FR_SCORE,
            NULL AS FR_SCORED_BY,
            NULL AS FR_SCORE_DATE,
            NULL AS FR_TIMESTAMP,
            -- NULL AS FR_TITLE,
            'Subjective Score' AS KPI,
            NULL AS KPI_JOIN,
            -- NULL AS KT_KPI,
            NULL AS KT_METRIC_TARGET,
            -- NULL AS KT_MINIMUM_TARGET__DE,
            -- NULL AS KT_MINIMUM_TARGET__ST,
            -- NULL AS KT_PK,
            NULL AS KT_TARGET,
            LOAN_OFFICER_NAME,
            NULL AS ORIGINATION_TARGET,
            NULL AS ORIGINATION_TARGET_JOIN,
            -- NULL AS OT_CONCAT,
            -- NULL AS OT_EMPLOYEE,
            NULL AS OT_MONTH,
            NULL AS OT_TARGET,
            NULL AS ROLE,
            -- NULL AS ROSTER_CONTACT_ID,
            -- NULL AS ROSTER_CONTACT_ID__ST,
            -- NULL AS ROSTER_END_DATE,
            -- NULL AS ROSTER_END_DATE__TI,
            -- NULL AS ROSTER_LOAN_OFFICER_NAME,
            -- NULL AS ROSTER_START_DATE,
            NULL AS ROSTER_TEAM,
            'Subjective Score' AS SALES_KPI,
            NULL AS VALUE,
            null as actual_origination_pipeline
        FROM ODS.ODS_RETAIL_SCORECARD_BUILDER.ROW_LEVEL_RETAIL_SCORECARD
        GROUP BY DATEFRAME, LOAN_OFFICER_NAME
    
        UNION ALL
        -- Total Score
        SELECT 
            'Total Score' AS CS_KPI,
            -- NULL AS CS_LOWER_BOUND,
            NULL AS CS_MIN_TARGET__DE,
            NULL AS CS_MIN_TARGET__ST,
            -- NULL AS CS_PERCENT_OF_TARGET,
            -- NULL AS CS_PK,
            NULL AS CS_SCORE,
            NULL AS CS_TARGET,
            -- NULL AS CS_UPPER_BOUND,
            NULL AS CS_WEIGHT,
            (SUM(CS_WEIGHTED_SCORE) + SUM(FR_SCORE)/4)/2 AS CS_WEIGHTED_SCORE,
            DATEFRAME,
            -- NULL AS FR_LOAN_ADVISOR_NAME,
            NULL AS FR_QUESTION,
            NULL AS FR_RESPONSE,
            (SUM(CS_WEIGHTED_SCORE) + SUM(FR_SCORE) / 4) / 2 AS FR_SCORE,
            NULL AS FR_SCORED_BY,
            NULL AS FR_SCORE_DATE,
            NULL AS FR_TIMESTAMP,
            -- NULL AS FR_TITLE,
            'Total Score' AS KPI,
            NULL AS KPI_JOIN,
            -- NULL AS KT_KPI,
            NULL AS KT_METRIC_TARGET,
            NULL AS KT_TARGET,
            LOAN_OFFICER_NAME,
            NULL AS ORIGINATION_TARGET,
            NULL AS ORIGINATION_TARGET_JOIN,
            NULL AS OT_MONTH,
            NULL AS OT_TARGET,
            NULL AS ROLE,
            NULL AS ROSTER_TEAM,
            'Total Score' AS SALES_KPI,
            NULL AS VALUE,
            null as actual_origination_pipeline
        FROM ODS.ODS_RETAIL_SCORECARD_BUILDER.ROW_LEVEL_RETAIL_SCORECARD
        GROUP BY DATEFRAME, LOAN_OFFICER_NAME
    )
    SELECT *
    FROM summary
),

loan_officers as (
    select
    distinct
    lower(trim(r.loan_officer_name)) as loan_officer_name1
    from ods.ods_sales_roster.sales_roster r 

)





SELECT 
    case 
    when sales_kpi = 'Pipeline' then 'Pipeline'
    else CS_KPI end as CS_KPI,
    cs_score,
    cs_weight,
    CS_WEIGHTED_SCORE,
    VALUE,
    ot_target,
    actual_origination_pipeline,
    SALES_KPI,
    FR_SCORE,
    case when sales_kpi = 'Pipeline' then 150 -- modified this
    else KT_METRIC_TARGET end as KT_METRIC_TARGET, -- add 150 
    dateframe,
    FR_QUESTION,
    fr_response,
    LOAN_OFFICER_NAME,
    FR_SCORE_DATE,
    FR_SCOREd_by,
    FR_TIMESTAMP,
    case when l.loan_officer_name1 is not null then true else false end as loan_officer
FROM checkquery
join loan_officers l on l.loan_officer_name1 = lower(trim(LOAN_OFFICER_NAME))