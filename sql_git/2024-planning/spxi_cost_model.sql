with bill_fee_order_base as
(select 
    *
    ,first_value(city_name_full) over (partition by grass_date,partner_id order by order_id asc) as first_city_name_full
    

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
where 1=1
-- and source in ('Food','Market')
-- and status = 7
and grass_date between date '2023-10-01' and date '2023-10-31'
and coalesce(city_name_full,'na') not in ('na','TestCity','Dien Bien')
)
,driver_cost_raw as
(select 
    case 
        when first_city_name_full in ('HCM City','Ha Noi City' ,'Da Nang City') then first_city_name_full
        when first_city_name_full IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then 'T2'
        else 'T3' end as city_group
    ,case 
         when city_name IN ('HCM','HN','DN') THEN city_name_full 
         when city_name_full IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then 'T2'
         else 'T3'
         END AS cities
    ,(CASE 
        -- WHEN is_nan(bfo.bonus) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub
        ELSE null end)*1.000000/exchange_rate   as bonus_usd_all

        , (driver_cost_base + bfo.return_fee_share_basic)*1.000000/exchange_rate   as total_driver_cost_base_all
        , (driver_cost_surge + bfo.return_fee_share_surge)*1.000000/exchange_rate   as total_driver_cost_surge_all
        ,(CASE 
        -- WHEN is_nan(bfo.bonus_v2) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub_v2
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub_v2
        ELSE null end)*1.000000/exchange_rate   as bonus_usd_all_v2
        , (driver_cost_base_v2 + bfo.return_fee_share_basic)*1.000000/exchange_rate   as total_driver_cost_base_all_v2
        , (driver_cost_surge_v2 + bfo.return_fee_share_surge)*1.000000/exchange_rate   as total_driver_cost_surge_all_v2
            -- ,delivered_by
        ,case 
             
            when city_name_full in ('HCM City', 'Ha Noi City') then 
            (case
            when delivered_by = 'hub' then 'T1'
            when delivered_by != 'hub' and lower(new_driver_tier_v2) like '%hub%' then 'T1' --- outshift
            when delivered_by != 'hub' and new_driver_tier_v2 is null then 'T1'
            when delivered_by != 'hub' and new_driver_tier_v2 = 'part_time' then 'T1'
            else new_driver_tier_v2 end) 
            when city_name_full not in ('HCM City', 'Ha Noi City') then  'part_time' else null end
            driver_tier
        ,case
        when distance <= 8 then '1. <=8km'
        else '2++8km' end as distance_range
        ,*
from bill_fee_order_base bfo

where city_name_full in ('HCM City','Ha Noi City')
and source not in ('Food','Market')
)
select 
    -- coalesce(cities,'HCM & HN')||coalesce(distance_range,'All distance')||coalesce(driver_tier,'Overall') as lookup_value
     date_trunc('month',grass_date) as month_
    ,cities
    ,coalesce(distance_range,'All distance') as distance_range
    ,coalesce(driver_tier,'Overall') as tier
    ,count(distinct order_id)/cast(count(distinct grass_date) as double) as ado
    ,count(distinct case when is_stack_group_order = 0 then order_id else null end)/cast(count(distinct grass_date) as double) as single_ado
    ,count(distinct case when is_stack_group_order = 1 then order_id else null end)/cast(count(distinct grass_date) as double) as group_ado
    ,count(distinct case when is_stack_group_order = 2 then order_id else null end)/cast(count(distinct grass_date) as double) as stack_ado
    ,sum(total_driver_cost_base_all)/count(distinct order_id) as base_v1
    ,sum(total_driver_cost_surge_all)/count(distinct order_id) as surge_v1
    ,sum(bonus_usd_all)/count(distinct order_id) as bonus_v1
    ,sum(total_driver_cost_base_all_v2)/count(distinct order_id) as base_v2
    ,sum(total_driver_cost_surge_all_v2)/count(distinct order_id) as surge_v2
    ,sum(bonus_usd_all_v2)/count(distinct order_id) as bonus_v2
    
    
    
from driver_cost_raw
group by 1,2, grouping sets (distance_range,driver_tier,(distance_range,driver_tier),()) 
UNION ALL
select 
    -- coalesce(cities,'HCM & HN')||coalesce(distance_range,'All distance')||coalesce(driver_tier,'Overall') as lookup_value
     date_trunc('month',grass_date) as month_
    ,'HCM & HN'cities
    ,coalesce(distance_range,'All distance') as distance_range
    ,coalesce(driver_tier,'Overall') as tier
    ,count(distinct order_id)/cast(count(distinct grass_date) as double) as ado
    ,count(distinct case when is_stack_group_order = 0 then order_id else null end)/cast(count(distinct grass_date) as double) as single_ado
    ,count(distinct case when is_stack_group_order = 1 then order_id else null end)/cast(count(distinct grass_date) as double) as group_ado
    ,count(distinct case when is_stack_group_order = 2 then order_id else null end)/cast(count(distinct grass_date) as double) as stack_ado
    ,sum(total_driver_cost_base_all)/count(distinct order_id) as base_v1
    ,sum(total_driver_cost_surge_all)/count(distinct order_id) as surge_v1
    ,sum(bonus_usd_all)/count(distinct order_id) as bonus_v1
    ,sum(total_driver_cost_base_all_v2)/count(distinct order_id) as base_v2
    ,sum(total_driver_cost_surge_all_v2)/count(distinct order_id) as surge_v2
    ,sum(bonus_usd_all_v2)/count(distinct order_id) as bonus_v2
    
    
    
from driver_cost_raw
group by 1,2, grouping sets (distance_range,driver_tier,(distance_range,driver_tier),())    
