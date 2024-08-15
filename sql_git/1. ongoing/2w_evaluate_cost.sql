with raw as 
(select 
        bf.grass_date,
        bf.source,
        bf.distance,
        bf.order_id ,
        dot.driver_distance,
        bf.city_name,
        case 
        when dot.driver_distance <= 1 then '1.1km'
        when dot.driver_distance <= 2 then '2.2km'
        when dot.driver_distance <= 3 then '3.3km'
        when dot.driver_distance <= 4 then '4.4km'
        when dot.driver_distance <= 5 then '5.5km'
        when dot.driver_distance <= 7 then '6.7km'
        when dot.driver_distance <= 19 then '7.19km'
        when dot.driver_distance <= 15 then '8.15km'
        when dot.driver_distance <= 20 then '9.20km'
        when dot.driver_distance > 20 then '10. ++20km' end as distance_range,
        bf.is_qualified_hub,
        delivered_by,
        driver_cost_base_n_surge + bonus as driver_cost_base_n_surge_bonus ,
        if(is_stack_group_order in (1,2),1,0) as is_stack_group_order

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join 
(select ref_order_id,ref_order_code,delivery_distance/1000.00 as driver_distance,ref_order_category

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
where date(dt) = current_date - interval '1' day 
) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = bf.ref_order_category

where bf.grass_date between date'2024-08-13' - interval '7' day and date'2024-08-13'
-- and source = 'Food'
)
select 
        grass_date,
        source,
        distance_range,
        coalesce(city_name,'VN') as cities,
        count(distinct order_id) as ado,
        count(distinct case when delivered_by = 'hub' then order_id else null end) as hub_ado,
        count(distinct case when delivered_by != 'hub' then order_id else null end) as non_hub_ado,
        count(distinct case when is_qualified_hub = 1 then order_id else null end) as hub_qualified,
        count(distinct case when is_qualified_hub = 0 then order_id else null end) as hub_non_qualified,
        sum(driver_cost_base_n_surge_bonus)*1.0000/count(distinct order_id) as cpo_overall,
        sum(case when delivered_by = 'hub' then driver_cost_base_n_surge_bonus else null end)*1.0000
                /count(distinct case when delivered_by = 'hub' then order_id else null end) as cpo_hub,
        sum(case when delivered_by != 'hub' then driver_cost_base_n_surge_bonus else null end)*1.0000
                /count(distinct case when delivered_by != 'hub' then order_id else null end) as cpo_non_hub,
        count(distinct case when is_stack_group_order = 1 then order_id else null end) as stack,
        count(distinct case when is_stack_group_order = 1 and delivered_by = 'hub' then order_id else null end) as hub_stack,
        count(distinct case when is_stack_group_order = 1 and delivered_by != 'hub' then order_id else null end) as non_hub_stack

from raw 
group by 1,2,3,grouping sets (city_name,())






