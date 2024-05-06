WITH driver_cost_base as 
(select 
    bf.*
                                                          
                                                                                      
                                              
    ,(driver_cost_base + return_fee_share_basic) as dr_cost_base
    ,(driver_cost_surge + return_fee_share_surge) as dr_cost_surge
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)   as dr_cost_bonus
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end) as dr_cost_bw_fee
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end) as dr_cost_late_night
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end) as dr_cost_holiday_fee

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
                                                                                                                   
WHERE grass_date != date'2023-06-06'
AND status = 7
AND source in ('Food')
AND city_name_full is not null
)
select 
    date_trunc('month',grass_date) as month_
    ,if(is_stack_group_order = 0,'single','stack|group') as assign_type
    ,coalesce(city_name_full,'VN') as cities
    ,count(distinct order_id) AS total_orders
    ,(sum(dr_cost_base) + sum(dr_cost_surge) + sum(dr_cost_bonus)) as driver_cpo_base_surge_bonus
    ,(sum(dr_cost_base)+ sum(dr_cost_surge) ) AS base_n_surge
    ,sum(dr_cost_base) AS base 
    ,sum(dr_cost_surge) AS surge
    ,sum(dr_cost_bonus) AS bonus 

    ,sum(dr_cost_bw_fee) AS bwf 
    ,sum(dr_cost_late_night) AS late_night 
    ,sum(dr_cost_holiday_fee) AS holiday_fee

    ,count(distinct case when dr_cost_bw_fee > 0 then order_id else null end) AS bwf_order 
    ,count(distinct case when dr_cost_late_night > 0 then order_id else null end) AS late_night_order
    ,count(distinct case when dr_cost_holiday_fee > 0 then order_id else null end) AS holiday_fee_order

    ,COUNT(DISTINCT grass_date) as days 

FROM driver_cost_base
where grass_date between date'2022-12-01' and date'2024-04-30'

GROUP BY 1,2,grouping sets (city_name_full,())
