with tata_mandate_data as
( SELECT application_id,
created_date_time::timestamp + interval '5 hour 30 minutes' as created_date_time,
bank_account_number,
bank_ifsc_code
, mandate_status
, CAST(save_mandate_request AS JSONB)->>'mandate_request_id' as mandate_request_id
CAST(tata_mandate_data AS JSONB)->>'emandate_error_message' AS emandate_error_message,
CAST(tata_mandate_data AS JSONB)->>'emandate_status' AS emandate_status
FROM credit_applications_data_audit_tata_mandate
WHERE date(created_date_time::timestamp+ interval '5 hour 30 minutes') > CURRENT_DATE::timestamp+ interval '5 hour 30 minutes'- INTERVAL '6 days'
),
tata_mandate_summary as 
(
SELECT 'Tata' AS Lender
, DATE(created_date_time) AS Date
, date_trunc('week', created_date_time) as week_start
, date_trunc('month', created_date_time) as month_start
, COUNT(distinct mandate_request_id) AS Total_mandate_attempts_events
, COUNT(distinct case when mandate_status in ('Finished') then mandate_request_id else null end) AS Total_mandate_success_events
, COUNT(distinct case when mandate_status in ('Failed') then mandate_request_id else null end) AS Total_mandate_failed_events
, COUNT(DISTINCT application_id) AS Total_applications_attempts
, COUNT(DISTINCT CASE WHEN status='Completed' THEN application_id ELSE NULL END) AS Total_applications_success_attempts
, COUNT(DISTINCT CASE WHEN status='Failed' THEN application_id ELSE NULL END) AS Total_applications_failed_attempts
FROM  tata_mandate_data t2
GROUP BY 1
),
select 
, Date
, week_start
, month_start
, business_channel
, platforms
, Total_mandate_attempts_events
, Total_mandate_success_events
, Total_mandate_failed_events
, Total_applications_attempts
, Total_applications_success_attempts
, Total_applications_failed_attempts
from tata_mandate_summary
df_groupby = df.groupby(['date']).count()
df_groupby = df.groupby(['date','business_channel','platform']).count()
select mandate_status , count(*) from credit_applications_data_audit_tata_mandate group by 1
select application_id 
, count(case when mandate_status = 'In Progress' then application_id else null end ) as inprogress
, count(case when mandate_status = 'Finished' then application_id else null end ) as Finished
from  credit_applications_data_audit_tata_mandate
group by 1
select application_id 
, CAST(save_mandate_request AS JSONB)->>'mandate_request_id' AS mandate_request_id 
, mandate_status 
, last_updated_on
, save_mandate_request
, save_mandate_response
from credit_applications_data_audit_tata_mandate
where application_id = '42b028dc-81b3-4fba-acdd-09435f045441'
select * from credit_applications_data_audit_tata_mandate