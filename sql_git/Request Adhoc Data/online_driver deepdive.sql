with base as
(select
date(from_unixtime(create_time-3600)) create_time
,from_unixtime(create_time-3600) c_ts
--,id
-- ,previous_balance/100+ previous_deposit/100 as total_balance
,user_id
,sum(balance*1.000/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as balance
,row_number () over(partition by user_id order by from_unixtime(create_time-3600) desc) as rank
,sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as deposit
--,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) + sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as total_balance

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
where date(from_unixtime(create_time-3600)) < current_date
)
,final_metrics as 
(select 
        date(from_unixtime(a.report_date - 3600)) as report_date 
       ,a.uid as shipper_id 
       ,a.total_online_seconds/cast(3600 as double) as total_online_time 
       ,a.total_completed_order
       ,coalesce(lc.balance,0) as balance_at_end_date
       ,coalesce(lc.deposit,0) as deposit_at_end_date
       ,cast(try(lc.balance/lc.deposit) as double) as wallet_rate
       ,date(from_unixtime(spp.create_time - 3600)) as onboard_date
      ,case 
           when date_diff('day',date(from_unixtime(spp.create_time - 3600)),date(from_unixtime(a.report_date - 3600))) <= 30 then '1. <= 1 months' 
           when date_diff('day',date(from_unixtime(spp.create_time - 3600)),date(from_unixtime(a.report_date - 3600))) <= 180 then '2. 1 - 6 months' 
           when date_diff('day',date(from_unixtime(spp.create_time - 3600)),date(from_unixtime(a.report_date - 3600))) <= 270 then '3. 6 - 9 months'
           when date_diff('day',date(from_unixtime(spp.create_time - 3600)),date(from_unixtime(a.report_date - 3600))) <= 365 then '4. 9 - 12 months'
           when date_diff('day',date(from_unixtime(spp.create_time - 3600)),date(from_unixtime(a.report_date - 3600))) > 365 then '5. > 1 year'   end as seniority_range       
       ,count(case when issue_category = 'Ignore' then order_id else null end) as total_ignored 
       ,count(case when issue_category != 'Ignore' then order_id else null end) as total_denied  





from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live a

left join dev_vnfdbi_opsndrivers.phong_raw_assignment_test sa on sa.shipper_id = a.uid and sa.date_ = date(from_unixtime(a.report_date - 3600))

left join base lc on lc.user_id = a.uid and lc.rank = 1 and lc.create_time <= date(from_unixtime(a.report_date - 3600))

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = a.uid 

where 1 = 1 
and date(from_unixtime(a.report_date - 3600)) >= current_date - interval '14' day 
and total_completed_order = 0


group by 1,2,3,4,5,6,7,8,9
)

select 
        report_date
       ,seniority_range
       ,case when deposit_at_end_date = 0 and balance_at_end_date > 0 then 1
             when try(balance_at_end_date/deposit_at_end_date) > -0.75 then 1 else 0 end as is_valid_wallet
    --    ,shipper_id,deposit_at_end_date,balance_at_end_date
        ,case when total_ignored > 0 then 1 when total_denied > 0 then 1 else 0 end as is_have_ignore_denied
        ,count(distinct shipper_id) as total_drivers
        ,sum(total_online_time)/cast(count(distinct shipper_id) as double) as avg_online_time
        ,approx_percentile(total_online_time,0.95) as pct95th_online_time
        ,approx_percentile(total_online_time,0.9) as pct90th_online_time

from final_metrics

-- where shipper_id = 40063791	


group by 1,2,3,4
