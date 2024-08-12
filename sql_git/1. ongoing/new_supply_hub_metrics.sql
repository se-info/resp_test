with                    
registered_drivers as
(select 
                                                                    
    date_trunc('month',date(from_unixtime(sr.create_time-3600))) as report_month
    ,count(id) as registered_drivers
from shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live  sr
                
                                       
                                                                 
                 
                                                                          
                                                      
                              
                                                                  
where date(from_unixtime(sr.create_time-3600)) between date_trunc('month',current_date - interval '1' day) - interval '3' month and current_date - interval '1' day
                                  
group by 1
)
          
        
                          

          
                                            
                                             
                                             
         
                                                                                     
                                                                                                                                                                      
                             
            


,avaiable_drivers as
(select 
    try_cast(grass_date as date) as last_day_of_month
    ,date_trunc('month',try_cast(grass_date as date)) as report_month
    ,count(distinct shipper_id) as avaiable_drivers
from shopeefood.foody_mart__profile_shipper_master sm
inner join ( 
        select distinct report_date 
        from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim
        where 
        (case when report_date = current_date - interval '1' day then 1
        when report_date = last_day_of_month then 1
        else 0 end) = 1 ) d
    on try_cast(sm.grass_date as date) = d.report_date
where try_cast(grass_date as date) between date'2024-01-01' and current_date - interval '1' day
AND sm.shipper_status_code = 1 
group by 1,2
)

                  
,onboard_drivers as
(select 
    date_trunc('month',date(from_unixtime(create_time-3600))) as report_month
    ,count(distinct uid) as onboarding_drivers
FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live
where date(from_unixtime(create_time-3600)) >= date '2022-11-01' 
group by 1
)

                                                                                                
,order_base as
(select 
    date_trunc('month',report_date) report_month
    ,report_date
    ,count(distinct uid) net_orders
    ,count(distinct shipper_id) as trans_drivers
    ,count(distinct case when is_stack_order = 1 then uid else null end) as total_stack_orders
    ,count(distinct case when is_stack_order = 1 or is_group_order = 1 then uid else null end) as total_stack_group_orders
    ,count(distinct case when is_order_in_hub_shift = 1 then uid else null end) total_hub_orders
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev
where report_date between date'2024-01-01' and current_date - interval '1' day
and order_status = 'Delivered'
                            
group by 1,2
)
,drivers_utilization as
(select 
    report_month
    ,avg(net_orders) ado_net_order
    ,avg(trans_drivers) as avg_transacting_drivers
    ,avg(total_stack_group_orders) as avg_stack_group_orders
    ,avg(total_stack_orders) as avg_total_stack_orders
    ,avg(total_stack_group_orders) / avg(net_orders) as pct_stack_group
    ,avg(net_orders) / avg(trans_drivers) as net_ado
    ,avg(total_hub_orders) / avg(net_orders) as hub_coverage
from order_base
group by 1
)


                                                                                                                                    
,active_drivers as
(select 
    slot.grass_date
    ,slot.shipper_id
    ,case when test.shipper_id is not null then 1 else 0 end as is_test
    ,case when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as shipper_type
    ,sum(online_time) as total_online_time
    ,sum(work_time) as total_working_time
from vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot slot
left join shopeefood.foody_mart__profile_shipper_master sm
    on slot.shipper_id = sm.shipper_id and slot.grass_date = try_cast(sm.grass_date as date)
left join vnfdbi_opsndrivers.shopeefood_vn_bnp_testing_shipper test
    on slot.shipper_id = test.shipper_id
where slot.grass_date between date'2024-01-01' and current_date - interval '1' day
group by 1,2,3,4
)
,daily_active_drivers as
(select 
    grass_date
    ,count(distinct shipper_id) as active_drivers
    ,count(distinct case when total_online_time >= 7.5 then shipper_id else null end ) as full_time_drivers
    ,count(distinct case when total_online_time < 7.5 then shipper_id else null end ) as part_time_drivers
    ,count(distinct case when shipper_type = 'hub' then shipper_id else null end) as hub_drivers
    ,count(distinct case when shipper_type != 'hub' then shipper_id else null end) as non_hub_drivers
    ,sum(total_online_time) as total_online_time
    ,sum(case when shipper_type = 'hub' then total_online_time else 0 end) as hub_online_time
    ,sum(case when shipper_type != 'hub' then total_online_time else 0 end) as non_hub_online_time
    ,sum(total_working_time) as total_working_time
    ,sum(case when shipper_type = 'hub' then total_working_time else 0 end) as hub_total_working_time
    ,sum(case when shipper_type != 'hub' then total_working_time else 0 end) as non_hub_total_working_time
    ,sum(case when total_working_time > 0 then total_working_time else null end )/sum(case when total_working_time > 0 then total_online_time else null end) as trans_driver_utilization
    ,sum(case when total_working_time > 0 then total_online_time else null end ) as supply_of_transacting
    ,sum(case when total_working_time > 0 and shipper_type = 'hub' then total_online_time else null end ) as hub_supply_of_transacting
    ,sum(case when total_working_time > 0 and shipper_type != 'hub' then total_online_time else null end ) as non_hub_supply_of_transacting
    ,count(distinct case when shipper_type = 'hub' and total_working_time > 0 then shipper_id else null end) as hub_drivers_working
    ,count(distinct case when shipper_type != 'hub' and total_working_time > 0 then shipper_id else null end) as non_hub_drivers_working

from active_drivers
                              
group by 1
)
,driver_profile as
(select 
    date_trunc('month',grass_date) as report_month
    ,avg(active_drivers) as avg_active_drivers
    ,avg(full_time_drivers) as avg_full_time_drivers
    ,avg(part_time_drivers) as avg_part_time_drivers
    ,avg(hub_drivers) as avg_hub_drivers
    ,sum(total_working_time)/sum(total_online_time) active_driver_utlization
    ,sum(hub_total_working_time)/sum(hub_online_time) hub_utlization
    ,sum(non_hub_total_working_time)/sum(non_hub_online_time) non_hub_utlization
    ,sum(total_online_time) / sum(active_drivers) as active_driver_online_time
    ,sum(hub_online_time) / sum(hub_drivers) as hub_online
    ,sum(non_hub_online_time) / sum(non_hub_drivers) as non_hub_online
    ,sum(hub_supply_of_transacting) / sum(hub_drivers_working) as hub_supply_of_transacting
    ,sum(non_hub_supply_of_transacting) / sum(non_hub_drivers_working) as non_hub_supply_of_transacting
    ,avg(hub_drivers_working) as hub_transacting
    ,avg(non_hub_drivers_working) as non_hub_transacting

from daily_active_drivers
group by 1
)



                    
,tip_txn as
(select 
        user_id as partner_id
        ,reference_id as order_id
                                                                   
        ,sum((balance + deposit)*1.00/100) as tip_
        ,count(distinct reference_id) as tip_orders
    from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
    where 1=1
    and txn_type in (135,110)

    group by 1,2
)
,tip_base as
(select 
    bf.grass_date
    ,bf.partner_id
    ,exchange_rate
    ,count(distinct case when t.tip_ > 0 then bf.order_id else null end) as tip_orders
    ,count(distinct bf.order_id) as total_orders
    ,sum(tip_) as tip_
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join tip_txn t
    on bf.order_id = t.order_id and bf.partner_id = t.partner_id
where 1=1
and grass_date between date'2024-01-01' and current_date - interval '1' day
group by 1,2,3
)

,driver_income as
(select 
    i.grass_date
    ,i.partner_id
    ,i.current_driver_tier
    ,coalesce(t.tip_,0) tip_
    ,t.tip_orders
    ,t.total_orders
    ,t.exchange_rate
    ,sum(total_earning_before_tax) as total_income
    ,sum(shipping_fee_share) as payout

from vnfdbi_opsndrivers.snp_foody_shipper_income_tab i

left join tip_base t
    on i.partner_id = t.partner_id and i.grass_date = t.grass_date
where i.grass_date between date'2024-01-01' and current_date - interval '1' day
group by 1,2,3,4,5,6,7
)

,driver_earning as
(select 
    date_trunc('month',grass_date) as report_month
    ,exchange_rate
    ,sum(total_income) / count(distinct (grass_date,partner_id))/exchange_rate as earning_per_day
    ,sum(case when current_driver_tier = 'Hub' then total_income else 0 end) / count(distinct case when current_driver_tier = 'Hub' then (grass_date,partner_id) else null end)/exchange_rate as hub_earning
    ,sum(case when current_driver_tier != 'Hub' then total_income else 0 end) / count(distinct case when current_driver_tier != 'Hub' then (grass_date,partner_id) else null end)/exchange_rate as non_hub_earning

    ,sum(case when current_driver_tier = 'Hub' then total_income else 0 end) / count(distinct case when current_driver_tier = 'Hub' then (partner_id) else null end)/exchange_rate as hub_earning_monthly
    ,sum(case when current_driver_tier != 'Hub' then total_income else 0 end) / count(distinct case when current_driver_tier != 'Hub' then (partner_id) else null end)/exchange_rate as non_hub_earning_monthly
    ,sum(payout) / count(distinct (grass_date,partner_id))/exchange_rate as earning_organic
    ,sum(total_income - payout - tip_) / count(distinct (grass_date,partner_id)) /exchange_rate as earning_non_organic
    ,sum(tip_) / count(distinct (grass_date,partner_id))/exchange_rate as tipping
    ,cast(sum(tip_orders) as double)/ sum(total_orders) pct_tip_order
    ,cast(sum(total_orders) as double) / count(distinct (grass_date,partner_id)) as net_ado_per_order
    ,sum(case when current_driver_tier = 'Hub' then total_orders else 0 end)*1.00000 / count(distinct case when current_driver_tier = 'Hub' then (grass_date,partner_id) else null end) as hub_ado
    ,sum(case when current_driver_tier != 'Hub' then total_orders else 0 end)*1.00000 / count(distinct case when current_driver_tier != 'Hub' then (grass_date,partner_id) else null end) as non_hub_ado
    ,cast(sum(tip_orders) as double)/ count(distinct (grass_date,partner_id))  as tip_order_per_driver
    
from driver_income
group by 1,2
)

select 
    ad.report_month
    ,ad.avaiable_drivers
    ,dp.hub_online
    ,dp.non_hub_online    
    ,dp.hub_utlization
    ,dp.non_hub_utlization
    ,dp.hub_transacting
    ,dp.non_hub_transacting
                          
    ,de.hub_earning_monthly
    ,de.non_hub_earning_monthly
    ,de.hub_earning
    ,de.non_hub_earning
    ,de.hub_ado
    ,de.non_hub_ado
    ,dp.hub_supply_of_transacting
    ,dp.non_hub_supply_of_transacting
    
        

from avaiable_drivers ad

left join driver_profile dp
    on ad.report_month = dp.report_month
    
left join driver_earning de
    on ad.report_month = de.report_month;
