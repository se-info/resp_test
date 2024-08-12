with raw as 
(select 
        raw.id,
        raw.group_id,
        raw.created_date,
        raw.created_timestamp,
        hour(raw.created_timestamp) as hour_,
        oct.restaurant_id as merchant_id,
        raw.sender_name as merchant_name,
        raw.city_name,
        case 
        when (raw.hub_id > 0 or raw.driver_policy = 2) then 1 else 0 end as is_hub_order,
        case 
        when raw.driver_policy = 2 then 1 else 0 end as is_hub,
        case 
        when raw.group_id > 0 and order_assign_type != 'Group' then 2
        when raw.group_id > 0 and order_assign_type = 'Group' then 1
        else 0 end as assign_type,
        case 
        when raw.delivered_timestamp > raw.eta_drop_time then 1 else 0 end as is_late_eta,
        case 
        when raw.distance <= 1 then 30
        when raw.distance > 1 then least(60,30 + 5*(ceiling(raw.distance) -1)) end as lt_sla,
        coalesce(IF(ms.prepare_time_actual is not null,ms.prepare_time_actual,date_diff('second',osl.confirmed_time,osl.picked_time)),0)*1.0000/60 as prepare_time,
        if(raw.order_status = 'Delivered',1,0) as is_del,
        if(raw.order_status = 'Cancelled',1,0) as is_cancel,
        if(raw.order_status = 'Quit',1,0) as is_quit,
        raw.is_asap,
        coalesce(da.is_da,0) as is_da,
        coalesce(if(mex_filter.restaurant_id is not null,1,0),0) as is_da_mex


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join 
(select
        case when delay_assign_enable = 1 and order_flow = 0 AND delay_assign_time > 0 then 1 else 0 end as is_da,
        order_id

FROM shopeefood_assignment.algo_delay_assign_order_data_vn 
where date(date_parse(dt,'%Y%m%d')) >= current_date - interval '60' day
) da on da.order_id = raw.id 

left join shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live ms 
    on ms.id = raw.id

left join 
(select 
        order_id,
        cast(max(case when status = 13 then from_unixtime(create_time) else null end) as timestamp) - interval '1' hour as confirmed_time,
        cast(max(case when status = 6 then from_unixtime(create_time) else null end) as timestamp) - interval '1' hour as picked_time

from shopeefood.foody_order_db__order_status_log_tab_di
where grass_date >= cast(current_date - interval '60' day as varchar)
group by 1 
) osl on osl.order_id = raw.id

left join (select id,restaurant_id from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day) oct 
    on oct.id = raw.id 


left join 
(select distinct restaurant_id 

from shopeefood_merchant.foody_merchant_db__restaurant_prepare_time_with_time_ranges_tab__reg_daily_s0_live
where setting_status = 1 
) mex_filter on mex_filter.restaurant_id = oct.restaurant_id


where raw.order_type = 0
and raw.grass_date >= current_date - interval '60' day
)
select 
        'merchant' as segment_level,
        created_date,
        city_name,
        cast(merchant_id as varchar) merchant_id,
        merchant_name,
        is_da as "1 is da 0 is non da",
        count(distinct id) as gross,
        count(distinct case when is_del = 1 then id else null end) as net,
        1.0000*sum(case when is_del = 1 and is_asap = 1 then prepare_time else null end)/count(distinct case when is_del = 1 and is_asap = 1 then id else null end) as avg_prep

from raw 
where is_da_mex = 1 
and created_date = date'2024-06-06'

group by 1,2,3,4,5,6
UNION ALL 
select 
        'city' as segment_level,
        created_date,
        city_name,
        'All' as merchant_id,
        'All' as merchant_name,
        is_da as "1 is da 0 is non da",
        count(distinct id) as gross,
        count(distinct case when is_del = 1 then id else null end) as net,
        1.0000*sum(case when is_del = 1 and is_asap = 1 then prepare_time else null end)/count(distinct case when is_del = 1 and is_asap = 1 then id else null end) as avg_prep

from raw 
where is_da_mex = 1 
and created_date = date'2024-06-06'

group by 1,2,3,4,5,6

