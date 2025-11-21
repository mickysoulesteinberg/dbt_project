select 
    billing_id,
    company_id,
    created_at as billing_date,
    0.75 as ai_billing_rate,
    ai_subtotal,
    total_billing
from {{ ref('ai_billing')}}