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
,base_2 as 
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

-- SELECT * from base_2 limit 200




select 
       a.grass_date
      ,case when is_same_pick = 1 then 'Pick (1) = Pick (2)'  ELSE 'Pick (1) <> Pick (2)' end as pick_route 
      ,case when is_same_drop = 1 then 'Drop (1) = Drop (2)'  ELSE 'Drop (1) <> Drop (2)' end as pick_route 
      ,sum(order_in_group) as total_order 




from base_2 a 

group by 1,2,3