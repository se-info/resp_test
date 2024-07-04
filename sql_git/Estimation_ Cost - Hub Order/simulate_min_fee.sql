with base as
(select 
    food.*
    ,date(from_unixtime(dot.submitted_time-3600)) as created_date
    ,dot.delivery_cost/100 as delivery_cost
    ,district.name_en as district_name
    --,if(distance <= 3 , '1. 0 - 3km', '2. 3km+') distance_range 
    ,case when distance between 0 and 2.3 then 1 
          when distance between 2.3 and 3 then 2 
          else 3 end as distance_range
    ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
    ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
    -- ,dotet.order_data
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level food
left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
    on food.order_id = dot.ref_order_id 
left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
    on dot.id = dotet.order_id
Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = food.pick_district_id
where food.grass_date between date '2022-12-01' and date '2022-12-31'
and city_name_full in ('Nghe An', 'Lam Dong')
and source in ('Food')
)
,shipping_fee_base as
(select 
    base.*
    ,GREATEST(min_fee,unit_fee*distance*surge_rate) as cal_system_param
    ,GREATEST(min_fee,(case when unit_fee = 3900 then 3900 else unit_fee end)*distance*surge_rate) as cal_opt_surge_current_3k9
    ,GREATEST(min_fee,3900*distance*surge_rate) as cal_opt_3k9
    ,GREATEST(min_fee,3800*distance*surge_rate) as cal_opt_3k8
    ,GREATEST(case when distance <= 2 then 12500 else 13500 end,3900*distance*surge_rate) as cal_opt_12k5_only
    ,GREATEST(case when distance <= 2 then 12500 else 13500 end,3800*distance*surge_rate) as cal_opt_12k5_n_3k8
    ,GREATEST(min_fee,(case when unit_fee = 3950 then 3950 else unit_fee end)*distance*1.45) as cal_opt_surge_new
    -- ,GREATEST(min_fee,(case when unit_fee = 4200 then 3800 else unit_fee end)*distance*surge_rate) as cal_opt3k8
    -- ,GREATEST(min_fee,(case when unit_fee = 4200 then 4000 else unit_fee end)*distance*surge_rate) as cal_opt4k0
    -- ,GREATEST(min_fee,(case when unit_fee = 4200 then 4200 else unit_fee end)*distance*surge_rate) as cal_opt4k2
from base
)
select 
    city_name_full
    ,count(distinct order_id) as total_orders
    ,sum(cal_system_param) cal_system_param
    ,sum(cal_opt_surge_current_3k9) as cal_opt_surge_current_3k9
    ,sum(cal_opt_3k9) cal_opt_3k9
    ,sum(cal_opt_3k8) cal_opt_3k8
    ,sum(cal_opt_12k5_only) cal_opt_12k5_only
    ,sum(cal_opt_12k5_n_3k8) cal_opt_12k5_n_3k8
    ,avg(distance) avg_distance
from shipping_fee_base
group by 1
limit 100
-- select 
--     b.*
--     ,case when district_name in ('Di An','Thuan An') then cal_opt_surge_new else cal_opt_surge_current end as cal_opt_surge_new_v2
-- from shipping_fee_base b
-- where cal_system_param <> cal_opt4k2


