WITH assignment AS 
(SELECT 
        *
        ,case when sa.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted
        ,case when sa.experiment_group in (5,6,7,8) then 1 else 0 end as is_ca
FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab sa 

WHERE 1 = 1 
AND sa.status in (3,4)
) 
,driver_cost_base as 
(select 
    bf.*
    ,case                                          
    when bf.grass_date = date'2023-08-31' then 'control'
    when bf.grass_date = date'2023-09-01' then 'treatment'
    when bf.grass_date = date'2023-09-02' then 'control'
    when bf.grass_date = date'2023-09-03' then 'treatment'
    when bf.grass_date = date'2023-09-04' then 'control'
    when bf.grass_date = date'2023-09-05' then 'treatment'
    when bf.grass_date = date'2023-09-06' then 'control'
    when bf.grass_date = date'2023-09-07' then 'treatment'
    when bf.grass_date = date'2023-09-08' then 'control'
    when bf.grass_date = date'2023-09-09' then 'treatment'
    when bf.grass_date = date'2023-09-10' then 'control'
    when bf.grass_date = date'2023-09-11' then 'treatment'
    when bf.grass_date = date'2023-09-12' then 'control'
    when bf.grass_date = date'2023-09-13' then 'treatment'
    when bf.grass_date = date'2023-09-14' then 'control'
    when bf.grass_date = date'2023-09-15' then 'treatment'
    when bf.grass_date = date'2023-09-16' then 'control'
    when bf.grass_date = date'2023-09-17' then 'treatment'
    when bf.grass_date = date'2023-09-18' then 'control'
    when bf.grass_date = date'2023-09-19' then 'treatment'
    when bf.grass_date = date'2023-09-20' then 'control'
    when bf.grass_date = date'2023-09-21' then 'treatment'
    when bf.grass_date = date'2023-09-22' then 'control'
    when bf.grass_date = date'2023-09-23' then 'treatment'
    when bf.grass_date = date'2023-09-24' then 'control' end as ab_version                                                                               
                                              
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee
    ,sa.is_ca

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

LEFT JOIN assignment sa 
    on sa.ref_order_id = bf.order_id
    and sa.order_category = bf.ref_order_category

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

where 1 = 1 
and bf.status = 7
and bf.source in ('Food')
and bf.grass_date between date'2023-08-31' and date'2023-09-24'
and bf.city_name in ('HCM','HN')
and sa_filter.order_id is null
)
select 
        grass_date,
        ab_version,
        -- delivered_by,
        -- is_ca,
        coalesce(city_name,'VN') as cities,
        count(distinct case when status = 7 then order_id else null end) as net,
        count(distinct case when status = 7 and delivered_by = 'hub' then order_id else null end) as hub_net,
        count(distinct case when status = 7 and delivered_by = 'hub' then order_id else null end)/cast(count(distinct order_id) as double) as pp_hub,
        count(distinct case when status = 7 and is_ca = 1 then order_id else null end)/cast(count(distinct order_id) as double) as pp_ca,
        (sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))*1.00000/count(distinct order_id) as cpo,
        (sum(dr_cost_base_usd) )*1.00000/count(distinct order_id) as base,
        (sum(dr_cost_surge_usd) )*1.00000/count(distinct order_id) as surge,
        (sum(dr_cost_bonus_usd) )*1.00000/count(distinct order_id) as bonus,

        (sum(case when delivered_by = 'hub' then dr_cost_base_usd else null end) + sum(case when delivered_by = 'hub' then dr_cost_surge_usd else null end) + sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else null end))
        /count(distinct case when delivered_by = 'hub' then  order_id else null end) as cpo_hub,
        (sum(case when delivered_by = 'hub' then dr_cost_base_usd else null end) )*1.00000/count(distinct case when delivered_by = 'hub' then  order_id else null end) as base_hub,
        (sum(case when delivered_by = 'hub' then dr_cost_surge_usd else null end) )*1.00000/count(distinct case when delivered_by = 'hub' then  order_id else null end) as surge_hub,
        (sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else null end) )*1.00000/count(distinct case when delivered_by = 'hub' then  order_id else null end) as bonus_hub,

        (sum(case when delivered_by != 'hub' then dr_cost_base_usd else null end) + sum(case when delivered_by != 'hub' then dr_cost_surge_usd else null end) + sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else null end))
        /count(distinct case when delivered_by != 'hub' then  order_id else null end) as cpo_non_hub,
        (sum(case when delivered_by != 'hub' then dr_cost_base_usd else null end) )*1.00000/count(distinct case when delivered_by != 'hub' then  order_id else null end) as base_non_hub,
        (sum(case when delivered_by != 'hub' then dr_cost_surge_usd else null end) )*1.00000/count(distinct case when delivered_by != 'hub' then  order_id else null end) as surge_non_hub,
        (sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else null end) )*1.00000/count(distinct case when delivered_by != 'hub' then  order_id else null end) as bonus_non_hub

from driver_cost_base

group by 1,2,grouping sets (city_name,())
;



