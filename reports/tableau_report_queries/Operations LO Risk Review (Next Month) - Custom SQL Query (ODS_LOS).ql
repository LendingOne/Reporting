select
    a.id as application_number,
    l.internal_id as loan_id,
    a.quoted_at,
    a.state,
    a.submitted_to_initial_underwriting_at,
    a.final_underwriting_received_date,
    a.funding_authorized_received_date,
    a.submitted_to_setup_servicing_at,
    a.clear_to_close_received_date,
    l.settlement_date,
    listagg(
        concat('commenter (', c.first_name, ' ', c.last_name, '): date (', n.created_at, '): note (', n.body, ')'),
        ' --> '
    ) within group (order by n.created_at) as concatenated_notes,
    snowflake.cortex.complete('mistral-large', concat('[INST]',
    'You are a Loan Processing Advisor for LendingOne designed to assist our processing team in completing loan applications by reviewing their current state. You provide a summary of the issues present in the notes and conversation provided and assign a risk level of Low, Medium, or High based on the severity of those issues. If present, make sure to include loan qualification, underwriting, appraisal, process and timeline issues.  Your responses should be clear, professional, and helpful, ensuring that users understand the issues that exist. Be specific in your response, include details like names, addresses, or other data items.  Do not use generic terms such as "client", "borrower", or "property" when specific details are available. Emphasize issues or blockers within the content provided and avoid speculation. Communicate in an informal but professional tone.  When responding include only a risk score, your reasoning for that risk score as a bulleted list of 2-3 sentences per bullet, and a summary of the issues as a bulleted list with 2-3 sentences for each bullet. Your response should be in the following JSON Format: {risk_score, reasons:[{reason}], issues:[{issue}]}',
    '[/INST]',
    concatenated_notes
    )) as risk_summary
from ods.ods_los.notes n
left join ods.ods_los.loans l on n.target_id = l.application_id
left join ods.ods_los.applications a on n.target_id = a.id
left join ods.ods_los.appraisal_refund_requests ar on ar.application_id = a.id
left join ods.ods_los.contacts c on n.author_id = c.id
where a.state in (
    'initial_underwriting',
    'pending_approval',
    'initial_terms',
    'initial_signed_term_sheet',
    'final_terms',
    'final_signed_term_sheet',
    'clear_to_close',
    'funding_requested',
    'funding_reviewed',
    'funding_submitted',
    'funding_sent',
    'funding_authorized'
)
and a.created_at > dateadd('day', -90,getdate())
and a.rejected_at is null
and a.withdrawn_at is null
and a.long_termed_at is null
and ar.refunded_at is null
group by a.id, l.internal_id, a.state, a.quoted_at, a.submitted_to_initial_underwriting_at, a.final_underwriting_received_date, a.funding_authorized_received_date, a.submitted_to_setup_servicing_at, a.clear_to_close_received_date, l.settlement_date
order by IFNULL(l.internal_id, 0) desc