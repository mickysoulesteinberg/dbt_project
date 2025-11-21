select company_id, min(billing_date) as start_date,
    min(case when ai_subtotal > 0 then billing_date end) as first_ai_billing_date
from {{ ref('stg_finance__billings')}}
group by company_id