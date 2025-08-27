select
l.internal_id
,date(l.appraisal_paid_at) as paid_appraisal_date
from ods.ods_los.loans l