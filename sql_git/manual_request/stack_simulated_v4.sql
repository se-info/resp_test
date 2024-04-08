with group_order_base as
(select 
    group_id
    ,count(distinct order_id) as order_in_group
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
group by 1
)
,merchant_in_group as
(
  select 
    group_id
    ,count(distinct merchant_id) as count_mex
  from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
  group by 1
)
,base as
(select 
    case when is_stack_group_order = 2 and grp.order_in_group = 2 then 1 else null end is_stack
    ,mi.count_mex
    ,grp.order_in_group
    ,bf.*
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
left join group_order_base grp
    on bf.group_id = grp.group_id
left join merchant_in_group as mi
  on mi.group_id = bf.group_id
where 1=1 
and bf.sub_source_v2 in ('Food','Market')
and grass_date between date '2022-07-01' and date '2022-07-31'
and grp.order_in_group >= 2
and delivered_by != 'hub'
)
,raw_group_level as
(select 
    grass_date
    ,group_id
    ,count_mex
    ,order_in_group
    ,exchange_rate
    ,sum(distance) as total_single_distance
    ,avg(distance_all) as distance_all
    ,avg(distance_grp) as distance_grp
    ,sum(driver_cost_base_n_surge) total_after_stack_fee_v1
    ,sum(total_shipping_fee_surge + total_shipping_fee_basic) as b_total_before_stack_fee
    
from base 
group by 1,2,3,4,5
)
,final as
(select 
    raw.grass_date
    ,raw.group_id
    ,raw.count_mex
    ,raw.order_in_group
    ,raw.total_single_distance
    ,raw.distance_all
    ,raw.distance_grp
    ,raw.exchange_rate
    ,json_extract(re.extra_data,'$.re') re_stack_system 
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as bigint) as surge_rate
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as bigint) as unit_fee
    ,case when json_extract_scalar(cast(json_extract(re.extra_data,'$.route') as array(json))[1],'$.address') = json_extract_scalar(cast(json_extract(re.extra_data,'$.route') as array(json))[2],'$.address') then 1 else 0 end as is_same_pick
    ,case when json_extract_scalar(cast(json_extract(re.extra_data,'$.route') as array(json))[3],'$.address') = json_extract_scalar(cast(json_extract(re.extra_data,'$.route') as array(json))[4],'$.address') then 1 else 0 end as is_same_drop

    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as bigint)*raw.distance_grp*cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as bigint) as a_spf_by_stacked_distance
    ,raw.b_total_before_stack_fee
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.min_fee') as bigint) as c_min_group_shipping_fee
    ,raw.total_after_stack_fee_v1

from raw_group_level raw
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re on re.id = raw.group_id
)
,summary as
(select 
    f.*
    ,greatest(least(a_spf_by_stacked_distance,b_total_before_stack_fee),c_min_group_shipping_fee) + 2000 as total_stack_fee_v2
    ,greatest(least(a_spf_by_stacked_distance,b_total_before_stack_fee),c_min_group_shipping_fee) + if(is_same_pick = 1,0,1000) + if(is_same_drop = 1,0,1000) as total_stack_fee_v3
    ,greatest(least(a_spf_by_stacked_distance,b_total_before_stack_fee),c_min_group_shipping_fee) as total_stack_fee_v4
from final f
)
---allocate new stack fee by order level
,bill_fee_raw as
(
    select 
        bf.grass_date
        ,bf.order_id
        ,bf.exchange_rate
        ,bf.distance
        ,bf.is_stack_group_order
        ,case when bf.distance <= 1 then '1. 0 - 1km'
            when bf.distance <= 2 then '2. 1 - 2km'
            when bf.distance <= 3 then '3. 2 - 3km'
            when bf.distance <= 4 then '4. 3 - 4km'
            when bf.distance <= 5 then '5. 4 - 5km'
            when bf.distance <= 6 then '6. 5 - 6km'
            when bf.distance <= 7 then '7. 6 - 7km'
            when bf.distance <= 8 then '8. 7 - 8km'
            when bf.distance <= 9 then '9. 8 - 9km'
            when bf.distance <= 10 then '10. 9 - 10km'
            when bf.distance > 10 then '11. > 10km'
            else null end distance_range

        ,case when s.group_id is null then driver_cost_base_n_surge else b_total_before_stack_fee*bf.distance/bf.distance_all end as b_total_before_stack_fee
        ,case when s.group_id is null then driver_cost_base_n_surge else total_after_stack_fee_v1*bf.distance/bf.distance_all end as total_after_stack_fee_v1
        ,case when s.group_id is null then driver_cost_base_n_surge else total_stack_fee_v2*bf.distance/bf.distance_all end as total_stack_fee_v2
        ,case when s.group_id is null then driver_cost_base_n_surge else total_stack_fee_v3*bf.distance/bf.distance_all end as total_stack_fee_v3
        ,case when s.group_id is null then driver_cost_base_n_surge else total_stack_fee_v4*bf.distance/bf.distance_all end as total_stack_fee_v4

        ,case when s.group_id is not null then 1 else 0 end as is_order_simulate
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
    left join summary s
        on bf.group_id = s.group_id
    where 1=1 
    and bf.sub_source_v2 in ('Food','Market')
    and bf.grass_date between date '2022-07-01' and date '2022-07-31'
)

select 
    grass_date
    ,distance_range
    ,count(distinct case when is_stack_group_order = 2 then order_id else null end ) as total_stack_orders
    ,count(distinct order_id ) as total_del_orders
    ,sum(b_total_before_stack_fee)/exchange_rate as total_before_stack_fee
    ,sum(total_after_stack_fee_v1)/exchange_rate as total_after_stack_fee_current
    ,sum(total_stack_fee_v2)/exchange_rate as option1
    ,sum(total_stack_fee_v3)/exchange_rate as option2
    ,sum(total_stack_fee_v4)/exchange_rate as option3
from bill_fee_raw
group by 1,2,exchange_rate
