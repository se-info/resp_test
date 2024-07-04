with raw as 
(select 
        a.id,
        a.order_code,
        a.group_id,
        a.order_type,
        case when a.city_id = dp.city_id then p.point
        else 0 end as original_point,
        a.shipper_id,
        date(a.delivered_timestamp) as report_date,
        case when a.city_id = dp.city_id then (
        case when a.order_type = 0 then p.point
        when p.point = 10 then 10
        when p.point = 12 then 10
        when p.point = 14 then 12 
        when p.point = 16 then 14
        end) else 0 end as point_v1,
        case when a.city_id = dp.city_id then (
        case
        when a.order_type = 0 then p.point
        else p.point - 2 end) else 0 end as point_v2,
        case when a.city_id = dp.city_id then (
        case 
        when a.order_type = 0 then p.point
        else 8 end) else 0 end as point_v3,
        from_unixtime(p.create_time - 3600) as c

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = a.shipper_id
        and dp.report_date = date(a.delivered_timestamp)

left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live p 
        on p.order_id = a.id
        and p.order_type = a.order_type

where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered','Quit')
                              
                                                     
)
,f as 
(select 
        raw.report_date,
        raw.shipper_id,
        dp.city_id,
        dp.shipper_tier,
        dp.daily_point,
        dp.sla_rate,
        dp.l30d_point,
        dp.driver_daily_bonus,
        dp.driver_income,
        count(distinct raw.order_code) as ado,
        sum(raw.original_point) as actual_earned_point,
        sum(raw.point_v1) as point_v1,
        sum(raw.point_v2) as point_v2,
        sum(raw.point_v3) as point_v3


from raw 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = raw.shipper_id
        and dp.report_date = raw.report_date

where raw.report_date between date_trunc('month',current_date) - interval '2' month and current_date - interval '1' day
and dp.shipper_tier != 'Hub'
and dp.city_id in (217,218)
group by 1,2,3,4,5,6,7,8,9
)
                                              
,s as 
(select 
        f.report_date,
        f.shipper_id,
        f.shipper_tier as actual_tier,
        f.l30d_point,
        f.actual_earned_point,
        f.point_v1,      
        f.city_id, 
        f.driver_daily_bonus,
        f.driver_income,
        f.ado,
        sum(f2.actual_earned_point) as actual_l30d_point,
        sum(f2.point_v1) as actual_point_v1
                                               
                                              


from f 

left join f as f2 
        on f2.shipper_id = f.shipper_id 
        and f2.report_date between f.report_date - interval '30' day and f.report_date - interval '1' day
where f.report_date >= date'2024-04-03'
group by 1,2,3,4,5,6,7,8,9,10
)
,final_ as 
(select 
        s.*,
        o.tier_name_en as original_tier,
        v1.tier_name_en as tier_v1
                                      

from s 

left join shopeefood.foody_internal_db__shipper_tier_config_tab__reg_daily_s0_live o on s.city_id = o.city_id and s.actual_l30d_point between o.from_total_point and o.to_total_point 

left join shopeefood.foody_internal_db__shipper_tier_config_tab__reg_daily_s0_live v1 on s.city_id = v1.city_id and s.actual_point_v1 between v1.from_total_point and v1.to_total_point                              
)
,s2 as 
(select 
        f.*,
        cast(i.bonus as double) as new_bonus,
        f.driver_income - (driver_daily_bonus - cast(i.bonus as double)) as new_income



from final_ f 

left join (select *,
                case 
                when current_driver_tier = 'T1' then 'Level 1'
                when current_driver_tier = 'T2' then 'Level 2'
                when current_driver_tier = 'T3' then 'Level 3'
                when current_driver_tier = 'T4' then 'Level 4'
                when current_driver_tier = 'T5' then 'Level 5' end as tier_mapping
         from vnfdbi_opsndrivers.ingest_non_hub_bonus_point) i 
        on f.actual_tier = i.tier_mapping
        and f.point_v1 between cast(i.from_ as bigint) and cast(i.to_ as bigint)
) 
-- select * from s2
select
        report_date,
        count(distinct shipper_id) as non_hub_a1,                                                                      
        count(distinct case when driver_daily_bonus != new_bonus then shipper_id else null end) as impacted_drivers,
        sum(driver_daily_bonus) as current_driver_daily_bonus,
        sum(new_bonus) as new_driver_daily_bonus,
        sum(driver_income)*1.00/count(distinct shipper_id) as current_income,
        sum(new_income)*1.00/count(distinct shipper_id) as new_income,
        sum(ado) as ado,
        sum(case when driver_daily_bonus != new_bonus then driver_income else null end)*1.00
        /count(distinct case when driver_daily_bonus != new_bonus then shipper_id else null end) as current_income_impacted,
        sum(case when driver_daily_bonus != new_bonus then new_income else null end)*1.00
        /count(distinct case when driver_daily_bonus != new_bonus then shipper_id else null end) as new_income_impacted


from s2
group by 1;