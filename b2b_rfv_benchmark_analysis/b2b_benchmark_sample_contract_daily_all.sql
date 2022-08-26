----with the contract daily table we grab the daily values from lighthouse for assigned, active and engaged users
SELECT distinct s.accountid, s.accountname, s.sub_status, s.sub_contractnumber, s.sub_type,s.sub_licensetype, s.contract_type, s.is_education
, s.sub_startdate, s.sub_enddate,month_duration,next_actions,ccr, s.active_readers, s.actual_core_readers, s.licensed_readers, s.assigned_readers
,user_status_date, date
, DENSE_RANK() OVER (PARTITION BY s.sub_contractnumber ORDER BY date(user_status_date) ASC) as rank_day
, count_users, user_engaged, rfv_engaged_readers as user_engaged_daily, m.active_readers as active_readers_daily, m.assigned_readers as assigned_readers_daily,
median_rfv_score, median_onsite_volume, median_onsite_frequency, median_email_volume, median_email_frequency,rfv_twentile
FROM `ft-customer-analytics.wp_gcaimmi.b2b_benchmark_sample_contract_daily` s
INNER JOIN `ft-lighthouse.lighthouse.dim_subscription` l on l.sub_contractnumber= s.sub_contractnumber
INNER JOIN `ft-lighthouse.lighthouse.subscription_metrics` m on m.dim_subscription_skey = l.dim_subscription_skey
INNER JOIN `ft-bi-team.BI_Layer.dim_date` d on d.date_dkey= m.date_dkey and d.date=s.user_status_date
order by accountname, rank_day asc
