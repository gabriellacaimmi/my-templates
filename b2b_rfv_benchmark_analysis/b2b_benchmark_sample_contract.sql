----we use the sample table which includes user_guids to find rfv scores in the known_user_daily_status_table
with table_base as (SELECT distinct 
accountid, accountname, sub_status, sub_contractnumber, sub_type, sub_startdate
, sub_enddate, sub_licensetype, contract_type, is_education, ccr, active_readers, actual_core_readers, licensed_readers, currently_assigned_readers as assigned_readers
, engaged_readers
, month_duration,next_actions
, count(distinct ft_user_id) as count_users
, count(distinct case when engagement_user_engaged = true then ft_user_id end ) as user_engaged 
, avg(engagement_rfv_score) as avg_rfv_score
,APPROX_QUANTILES(engagement_rfv_score, 1000)[OFFSET(500)] as median_rfv_score
, avg(engagement_onsite_volume) as avg_onsite_volume
,APPROX_QUANTILES(engagement_onsite_volume, 1000)[OFFSET(500)] as median_onsite_volume
, avg(engagement_onsite_frequency) as avg_onsite_frequency
,APPROX_QUANTILES(engagement_onsite_frequency, 1000)[OFFSET(500)] as median_onsite_frequency
, avg(engagement_email_volume) as avg_email_volume
,APPROX_QUANTILES(engagement_email_volume, 1000)[OFFSET(500)] as median_email_volume
, avg(engagement_email_frequency) as avg_email_frequency
,APPROX_QUANTILES(engagement_email_frequency, 1000)[OFFSET(500)] as median_email_frequency
FROM `ft-customer-analytics.wp_gcaimmi.b2b_benchmark_sample` s
INNER JOIN `ft-bi-team.BI_Layer_Integration.known_user_daily_status` u on s.ft_user_id = u.user_guid 
  and (user_status_date >= date(sub_startdate) and user_status_date > date(firstreg_dtm)) and user_status_date <= date(sub_enddate) ---including activity of users only during contract period
  and b2b_b2c= 'B2B' and visitor_id is not null --- including activity of B2B users only and with a visitor_id already created
  and product_start_date>= date(sub_startdate) and product_start_date<= date(sub_enddate) 
  and date(behav_last_visit) >= date(sub_startdate) --- including activity of users after they have joined the contract
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
order by accountname)

SELECT distinct *
, NTILE(20) OVER (ORDER BY median_rfv_score asc) as rfv_twentile
FROM table_base 
