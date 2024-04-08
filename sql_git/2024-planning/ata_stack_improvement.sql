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
,order_level as
(select 
    date_trunc('month',o.report_date) as report_month
    ,o.grass_date
    ,lt_incharge
    ,lt_incharge_to_arrive_at_merchant
    ,lt_pick_to_arrive_at_buyer
    ,lt_arrive_at_merchant_to_pick
    ,lt_arrive_at_buyer_to_del
    ,lt_completion_original/60.0000 lt_completion_original
    ,o.is_asap
    ,o.distance

    ,o.ref_order_id
    ,date_diff('second',from_unixtime(dot.submitted_time-3600), p.received_time)/60.0000 as lt_payment
    ,date_diff('second',p.received_time,from_unixtime(go.confirm_timestamp))/60.0000 as lt_merchant_confirm
    ,case 
        when o.distance < 1 then '1. 0-1km'
        when o.distance < 3 then '2. 1-3km'
        when o.distance < 5 then '3. 3-5km'
        when o.distance < 10 then '4. 5-10km'
        when o.distance >= 10 then '5. ++10km'
        end as distance_range
    ,case when dot.real_drop_time = 0 then null else cast(cast(from_unixtime(dot.real_drop_time - 3600) as timestamp) as timestamp) end as last_delivered_timestamp
    ,case when dot.estimated_drop_time = 0 then null else cast(cast(from_unixtime(dot.estimated_drop_time - 3600) as timestamp) as timestamp) end as estimated_delivered_time
    ,case when dot.real_drop_time != 0  and dot.estimated_drop_time != 0 and dot.estimated_drop_time is not null then date_diff('second',from_unixtime(dot.estimated_drop_time - 3600),from_unixtime(dot.real_drop_time-3600))/60.0000 else null end as ata_eta
    ,case when is_stack_order = 1 or is_group_order = 1 then 1 else 0 end as is_stack_group_order
    ,lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del as driver_to_buyer
    -- ,case when date_diff('second',p.received_time,from_unixtime(go.confirm_timestamp))/60.0000
    
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on o.ref_order_id = dot.ref_order_id and dot.ref_order_category = 0
left join (select 
    order_id
    ,min(from_unixtime(create_time-3600)) as received_time --> start payment
from shopeefood.foody_order_db__order_status_log_tab_di
where 1=1
and status = 2 
group by 1) p
    on dot.ref_order_id = p.order_id and dot.ref_order_category = 0
left join shopeefood.foody_mart__fact_gross_order_join_detail go
    on o.ref_order_id = go.id and o.source = 'NowFood'
where o.report_date between current_date - interval '90' day and current_date - interval '1' day
and o.source = 'NowFood'
and o.order_status = 'Delivered'
)
,base as
(select 
    case when bf.is_stack_group_order = 2 and grp.order_in_group = 2 then 1 else null end is_stack
    ,mi.count_mex
    ,grp.order_in_group
    ,bf.*
    ,od.lt_incharge_to_arrive_at_merchant
    ,od.lt_arrive_at_merchant_to_pick
    ,od.driver_to_buyer

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join order_level od 
    on od.ref_order_id = bf.order_id 

left join group_order_base grp
    on bf.group_id = grp.group_id
left join merchant_in_group as mi
  on mi.group_id = bf.group_id
where 1=1 
and bf.sub_source_v2 in ('Food','Market')
and bf.grass_date between date'2023-10-01' and current_date - interval '1' day
and grp.order_in_group >= 2
and od.is_asap = 1 
-- and bf.delivered_by != 'hub'
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
    ,sum(lt_incharge_to_arrive_at_merchant) as lt_incharge_to_arrive_at_merchant
    ,sum(lt_arrive_at_merchant_to_pick) as lt_arrive_at_merchant_to_pick
    ,sum(driver_to_buyer) as driver_to_buyer

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
    ,raw.lt_incharge_to_arrive_at_merchant
    ,raw.lt_arrive_at_merchant_to_pick
    ,raw.driver_to_buyer

from raw_group_level raw
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re on re.id = raw.group_id
)
select 
       DATE_TRUNC('month',grass_date) as period
      ,case when is_same_pick = 1 then 'Pick (1) = Pick (2)'  ELSE 'Pick (1) <> Pick (2)' end as pick_route 
    --   ,case when is_same_drop = 1 then 'Drop (1) = Drop (2)'  ELSE 'Drop (1) <> Drop (2)' end as drop_route 
      ,count(distinct group_id) as cnt_group
      ,sum(order_in_group) as cnt_order
      ,sum(lt_incharge_to_arrive_at_merchant)*1.0000/sum(order_in_group) as lt_incharge_to_arrive_at_merchant
      ,sum(lt_arrive_at_merchant_to_pick)*1.0000/sum(order_in_group) as lt_arrive_at_merchant_to_pick
      ,sum(driver_to_buyer)*1.0000/sum(order_in_group) as driver_to_buyer

      

from base_2 a 
group by 1,2


