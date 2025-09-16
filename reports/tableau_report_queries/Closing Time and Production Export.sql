SELECT role, 
'Closing Times' as kpi_type,
kpi as kpi_name, 
team_member_name,
ops_manager_name,
start_date as start_date,
end_date as end_date,
property_id,
application_id,
loan_id,
product as product,
null as channel,
aggregation_level,
kpi_count,
current_timestamp as last_update
from ods.ods_ops_scorecards.tbl_ops_closing_times

union all 

SELECT role,
'Production' as kpi_type,
kpi_name,
team_member_name,
ops_manager_name,
dateframe as start_date,
dateframe as end_date,
property_id,
application_id,
loan_id,
product,
channel,
'PRODUCTION UNIT' as aggregation_level,
1 as kpi_count,
current_timestamp as last_update
from ods.ods_ops_scorecards.tbl_ops_production