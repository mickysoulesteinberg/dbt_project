select company_id, stat_date, sum(ai_session_count) ai_session_count, sum(ai_resolved_count) ai_resolved_count
from `helpscout.daily_stats`
where stat_id is not null --remove nulls from google sheet
group by company_id, stat_date