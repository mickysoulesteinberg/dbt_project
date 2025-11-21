select billing_id, company_id, created_at, total_billing, ai_subtotal,
  date(date_trunc(created_at, month)) as billing_start_date,
  last_day(date(created_at)) as billing_end_date
from `helpscout.billings`
where billing_id is not null
order by company_id, created_at desc