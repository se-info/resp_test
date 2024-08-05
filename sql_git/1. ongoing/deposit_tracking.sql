with txn_tab as 
(select
        date(from_unixtime(create_time-3600)) create_time,
        date_trunc('month',date(from_unixtime(create_time-3600))) as month_
        ,id
        -- ,previous_balance/100+ previous_deposit/100 as total_balance
        ,user_id
        ,sum(balance*1.000/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as balance
        ,sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as deposit
        ,row_number()over(partition by user_id,date(from_unixtime(create_time-3600)) order by id desc) as rank_
        --,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) + sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as total_balance

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

where date(from_unixtime(create_time-3600)) != current_date 
)
,agg_tab as 
(select 
        shipper_id,
        array_agg(distinct dp.report_date) as delivered_date_agg

from driver_ops_driver_performance_tab dp 
where 1 = 1 
and total_order > 0 
group by 1 
)
,f as 
(select  
        date_trunc('month',dp.report_date) as month_,
        dp.report_date,
        dp.shipper_id,
        if(ds.is_available_spxi > 0,true,false) as is_available_spxi,
        if(cardinality(filter(agg.delivered_date_agg,x -> x = dp.report_date)) > 0,true,false) as is_a1,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between dp.report_date - interval '29' day and dp.report_date)) > 0,true,false) as is_a30,
        max_by(txn.deposit,txn.create_time) as deposit

from driver_ops_driver_performance_tab dp 

left join driver_ops_driver_services_tab ds on ds.uid = dp.shipper_id and ds.report_date = dp.report_date

left join agg_tab agg on agg.shipper_id = dp.shipper_id

left join txn_tab txn on txn.user_id = dp.shipper_id and txn.create_time <= dp.report_date and txn.rank_ = 1


where 1 = 1 
and dp.shipper_tier != 'Hub'
and dp.report_date between date_trunc('month',current_date) - interval '2' month and current_date - interval '1' day
and regexp_like(coalesce(dp.city_name,'na'),'na|Dien Bien|test|Test') = false
group by 1,2,3,4,5,6
)
-- select * from f where deposit is null and (is_a1 = true or is_a30 = true) LIMIT 200
select
        month_,
        case 
        when deposit <=0 then '1. Non Deposit'
        when deposit < 300000 then '2. 1 - 300k'
        when deposit < 500000 then '3. 300 - 500k'
        when deposit < 1000000 then '4. 500k - 1m'
        when deposit >= 1000000 then '5. ++5m'
        else null end as deposit_range,
        is_available_spxi,
        count(distinct (shipper_id,report_date))/cast(count(distinct report_date) as double) as existing_driver,
        count(distinct case when is_a1 = true then (shipper_id,report_date) else null end)
                /cast(count(distinct case when is_a1 = true then (report_date) else null end) as double) as a1,
        count(distinct case when is_a30 = true then (shipper_id,report_date) else null end)
                /cast(count(distinct case when is_a30 = true then (report_date) else null end) as double) as a30

from f 
where 1 = 1 
and (is_a1 = true or is_a30 = true)
and deposit is not null 
group by 1,2,3

-- # excluded hub and seperated by spxi avai/non avai