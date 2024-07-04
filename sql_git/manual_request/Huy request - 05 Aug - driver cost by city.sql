with driver_cost_base as 
(select 
    bf.*
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case when is_nan(bonus) = true then 0.00 else bonus end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
where date_ between current_date - interval '30' day and current_date - interval '1' day
)
,final as
(
select 
     b.date_
    ,case 
    when b.city_name_full in ('HCM City') then 'HCM'
    when b.city_name_full in ('Ha Noi City') then 'HN'
    when b.city_name_full in ('Da Nang City') then 'DN'
    else b.city_name_full end as city_category
    ,cast(count(distinct case when b.sub_source in ('Food') then  order_id else null end) as double) as food_total_orders
    ,cast(count(distinct case when b.sub_source in ('Food') and delivered_by = 'hub' then  order_id else null end) as double) as hub_food_total_orders
    ,cast(count(distinct case when b.sub_source in ('Food') and delivered_by != 'hub' then  order_id else null end) as double) as non_hub_food_total_orders
    -- total cost
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus
    
    -- non-hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_non_hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge_non_hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus_non_hub

    -- Hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_hub

    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge_hub

    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus_hub
    
    -- ,sum(case 
    --     when sub_source in ('Market') then
    --     (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
    --     else 0 end) as fresh_total_driver_cost

    -- ,sum (
    --     case when sub_source in ('Food') and delivered_by != 'hub' then (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) else 0 end
    -- ) as non_hub_food_total_driver_cost


from driver_cost_base b
where source in ('Food')
group by 1,2

union all

select 
      b.date_  
    ,'All' as city_category

    ,cast(count(distinct case when b.sub_source in ('Food') then  order_id else null end) as double) as food_total_orders
    ,cast(count(distinct case when b.sub_source in ('Food') and delivered_by = 'hub' then  order_id else null end) as double) as hub_food_total_orders
    ,cast(count(distinct case when b.sub_source in ('Food') and delivered_by != 'hub' then  order_id else null end) as double) as non_hub_food_total_orders

    -- total cost
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge
    ,sum(case 
        when sub_source in ('Food') then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus
    
    -- non-hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_non_hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge_non_hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by != 'hub' then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus_non_hub

    -- Hub
    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_hub

    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_base_usd + dr_cost_surge_usd) 
        else 0 end) as food_total_driver_cost_base_surge_hub
        
    ,sum(case 
        when sub_source in ('Food') and delivered_by = 'hub' then
        (dr_cost_bonus_usd) 
        else 0 end) as food_total_driver_cost_bonus_hub
    
    -- ,sum(case 
    --     when sub_source in ('Market') then
    --     (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) 
    --     else 0 end) as fresh_total_driver_cost

    -- ,sum (
    --     case when sub_source in ('Food') and delivered_by != 'hub' then (dr_cost_base_usd + dr_cost_surge_usd  + dr_cost_bonus_usd) else 0 end
    -- ) as non_hub_food_total_driver_cost


from driver_cost_base b
where source in ('Food')
group by 1,2

)
select 
    f.date_
    ,f.city_category

    -- drviers

    ,f.food_total_orders
    ,f.hub_food_total_orders
    ,f.non_hub_food_total_orders

    -- total driver cost
    ,f.food_total_driver_cost
    ,f.food_total_driver_cost_base_surge
    ,f.food_total_driver_cost_bonus

    -- non-hub
    ,f.food_total_driver_cost_non_hub
    ,f.food_total_driver_cost_base_surge_non_hub
    ,f.food_total_driver_cost_bonus_non_hub

    -- hub
    ,f.food_total_driver_cost_hub
    ,f.food_total_driver_cost_base_surge_hub
    ,f.food_total_driver_cost_bonus_hub

    -- 

    -- cpo
    -- ,f.food_total_driver_cost / cast(f.food_total_orders as double) as food_cpo_driver_vn 
    -- ,f.fresh_total_driver_cost/ cast(f.fresh_total_orders as double) as fresh_cpo_driver_vn
    -- ,f.non_hub_food_total_driver_cost/ cast(f.non_hub_food_total_orders as double) non_hub_food_cpo_driver_vn

from final f


where city_category is not null 

and city_category not in ('HCM','HN','DN')