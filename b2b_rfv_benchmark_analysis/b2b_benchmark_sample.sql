---- identify agency accounts that later will be excluded from analysis
with table_agency as (
SELECT distinct accountid
FROM `ft-lighthouse.lighthouse.dim_subscription`
WHERE sub_customer_category = 'Agency'
and sub_startdate > '2020-04-01'
)
,table_base as (
SELECT distinct d.accountid, d.accountname, d.sub_status, d.sub_contractnumber, d.sub_type, d.sub_startdate, d.sub_enddate, sub_licensetype
, case when d.sub_clienttype = 'Education' then 'Education' else 'no-Education' end as is_education
, case when sub_contractnumber like 'FT%' then 'FT'
when sub_contractnumber like 'Trial%' then 'Trial' end as contract_type
, sum(distinct sub_total_core_readers) as ccr
, sum(distinct sub_totallicensedreaders) as licensed_readers
, sum(distinct assigned_readers_latest) as currently_assigned_readers
, sum(distinct active_readers_latest) as active_readers
, sum(distinct rfv_engaged_readers_latest) as engaged_readers
,sum(distinct actual_core_readers_latest) as actual_core_readers
, round(date_diff(sub_enddate, sub_startdate, day)/30) as month_duration
FROM `ft-lighthouse.lighthouse.dim_subscription` d
INNER JOIN `ft-lighthouse.lighthouse.subscription_metrics` m on m.dim_subscription_skey = d.dim_subscription_skey
LEFT JOIN table_agency a on a.accountid = d.accountid
WHERE d.sub_type = 'New Business' --- cohort of interest
and (a.accountid is null OR a.accountid = '') ---excluding agency accounts
and d.sub_contractnumber not like 'BD-%' ----excluding business development accounts
and d.sub_contractnumber not like 'FTS-%' ----excluding FTS accounts
and sub_licencesolution = 'Digital Licence' ----only looking at digital licence contracts
and d.accountid != '-1'
and extract(date from d.sub_startdate) >= '2020-06-01'
and extract(date from d.sub_startdate) <= '2021-06-01'
group by 1,2,3,4,5,6,7,8,9,10
order by 5 asc
)

---ranking the contracts by start_date for any given account in order to filter for fresh businesses (rank=1)
, table_rank as (
SELECT distinct d.accountid, d.sub_contractnumber
, DENSE_RANK() OVER (PARTITION BY d.accountid ORDER BY d.sub_startdate ASC) as rank_number
FROM `ft-lighthouse.lighthouse.dim_subscription` d
INNER JOIN table_base b using (accountid)
)

----sum of contracts per rank to exclude accounts that had more than 1 contract in a given timeframe (agencies)
, table_sum_contracts as (
SELECT distinct b.accountid, accountname, rank_number, count(distinct r.sub_contractnumber) as sum_contracts
FROM table_base b
INNER JOIN table_rank r on b.accountid = r.accountid
group by 1,2,3
order by accountid, rank_number asc
)


SELECT distinct b.accountid, b.accountname, b.sub_status, b.sub_contractnumber, b.sub_type, b.sub_startdate, b.sub_enddate, b.sub_licensetype
, ft_user_id, u.firstreg_dtm
, is_education
, contract_type
, month_duration
, case when ra.sub_contractnumber is not null then "renewed" else "cancelled" end as next_actions
, sum(distinct ccr) as ccr
, sum(distinct licensed_readers) as licensed_readers
, sum(distinct engaged_readers) as engaged_readers
, sum(distinct currently_assigned_readers) as currently_assigned_readers
, sum(distinct active_readers) as active_readers
, sum(distinct actual_core_readers) as actual_core_readers
FROM table_base b
INNER JOIN table_rank r on b.accountid = r.accountid and b.sub_contractnumber= r.sub_contractnumber
INNER JOIN `ft-lighthouse.lighthouse.dim_subscription` d on b.sub_contractnumber= d.sub_contractnumber
INNER JOIN `ft-lighthouse.lighthouse.dim_user_subscription_bridge` br on br.dim_subscription_skey = d.dim_subscription_skey
INNER JOIN `ft-lighthouse.lighthouse.dim_user` u on u.dim_user_skey = br.dim_user_skey
LEFT JOIN table_sum_contracts c on c.accountid = b.accountid and c.rank_number = r.rank_number
LEFT JOIN table_rank ra on b.accountid = ra.accountid and ra.rank_number =2 -- identifying next contracts to see wether account remained
WHERE r.rank_number = 1
and sum_contracts = 1
and active_readers >=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by accountname
