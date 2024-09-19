with raw as 
(select 
        raw.created_date,
        date(raw.delivered_timestamp) as report_date,
        raw.group_id,
        raw.id,
        raw.order_code,
        raw.city_name,
        raw.city_id,
        raw.district_id,
        di.name_en as district_name,
        doet.unit_fee,
        doet.surge_rate,
        doet.min_fee,
        case 
        when doet.min_fee > if(city_id in (217,218),13500,12000) then 1 
        when doet.surge_rate > 1 then 2
        else 0 end as surge_type,
        raw.order_status,
        raw.is_no_driver,
        date_format(created_date,'%W') as "day_of_week",
        eta_drop_time,
        hour(eta_drop_time)*100+minute(eta_drop_time) as min_hour,
        doet.dotet_total_shipping_fee,
        driver_distance,
        bf.delivered_by,
        bf.driver_cost_base_n_surge,
        coalesce(raw.peak_mode_name,'Normal Mode') as peak_mode_name


from 
(select 
        raw.*,
        if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery,
        CASE 
        WHEN raw.source = 'now_ship_shopee' 
             THEN 
             (CASE 
             WHEN raw.order_status = 'Assigning Timeout' THEN 1 ELSE 0 END)
             WHEN raw.source in ('now_ship_user','now_ship_merchant') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null and sa.assigning_count > 0 
                     and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') 
                     THEN 1 ELSE 0 END)
            WHEN raw.source in ('now_ship_same_day') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') THEN 1 ELSE 0 END)
            WHEN raw.source in ('order_food','order_fresh','order_market') THEN 
                (CASE WHEN raw.cancel_reason = 'No driver' THEN 1 ELSE 0 END )
        ELSE 0 END AS is_no_driver


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '2' day) oct 
                on raw.id = oct.id
LEFT JOIN 
(SELECT 
            ref_order_id
        ,order_category
        ,COUNT(ref_order_id) AS assigning_count

FROM driver_ops_order_assign_log_tab
WHERE status in (3,4,2,14,15,8,9,17,18) 
GROUP BY 1,2
) sa on sa.ref_order_id = raw.id 
     and sa.order_category = raw.order_type
) raw 

left join 
(select 
        order_id,
        cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee,
        cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee,
        cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee,
        cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate


from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da dotet
where date(dt) = current_date - interval '2' day
) doet on doet.order_id = raw.delivery_id

left join vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf on bf.order_id = raw.id and bf.ref_order_category = raw.order_type

left join 
(select id,name_en,province_id
from shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live) di on di.id = raw.district_id and di.province_id = raw.city_id

where filter_delivery = 1
and order_type = 0
and created_date between date'2024-08-12' and date'2024-08-18'
)
select
        city_name,
        district_name,
        created_date,
        "day_of_week",
        count(distinct id) as ado,
        count(distinct case when peak_mode_name = 'Peak 3 Mode' then id else null end) as p3_ado,
        count(distinct case when is_affect = 1 and opt1 != min_fee then id else null end) as affected_opt1,
        count(distinct case when is_affect = 1 and opt2 != min_fee then id else null end) as affected_opt2,
        count(distinct case when is_affect = 1 and opt3 != min_fee then id else null end) as affected_opt3,
        count(distinct case when is_affect = 1 and opt3 != min_fee then id else null end) as affected_opt4,
        count(distinct case when is_affect = 1 and opt1_16k != driver_cost_base_n_surge then id else null end) as affected_opt1_16k,
        count(distinct case when is_affect = 1 and opt2_16k != driver_cost_base_n_surge then id else null end) as affected_opt2_16k,
        count(distinct case when is_affect = 1 and opt3_16k != driver_cost_base_n_surge then id else null end) as affected_opt3_16k,
        count(distinct case when is_affect = 1 and opt3_16k != driver_cost_base_n_surge then id else null end) as affected_opt4_16k,
        sum(driver_cost_base_n_surge) as total_shipping_fee_current,
        sum(opt1) as "total_shipping_fee_opt1 1.05",
        sum(opt2) as "total_shipping_fee_opt2 1.1",
        sum(opt3) as "total_shipping_fee_opt3 1.15",
        sum(opt4) as "total_shipping_fee_opt4 1.2",
        sum(opt1_16k) as "total_shipping_fee_opt1 1.05 - 16k",
        sum(opt2_16k) as "total_shipping_fee_opt2 1.1 - 16k",
        sum(opt3_16k) as "total_shipping_fee_opt3 1.15 - 16k",
        sum(opt4_16k) as "total_shipping_fee_opt4 1.2 - 16k"

from 
(select 
        city_name,
        district_name,
        created_date,
        "day_of_week",
        id,
        min_fee,
        driver_distance,
        group_id,
        driver_cost_base_n_surge,
        if((delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') ,0,1) as is_affect,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(min_fee,raw.unit_fee*1.05*raw.driver_distance) 
        end as opt1,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(min_fee,raw.unit_fee*1.1*raw.driver_distance) 
        end as opt2,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(min_fee,raw.unit_fee*1.15*raw.driver_distance) 
        end as opt3,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(min_fee,raw.unit_fee*1.2*raw.driver_distance) 
        end as opt4,
        
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(16000,raw.unit_fee*1.05*raw.driver_distance) 
        end as opt1_16k,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(16000,raw.unit_fee*1.1*raw.driver_distance) 
        end as opt2_16k,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(16000,raw.unit_fee*1.15*raw.driver_distance) 
        end as opt3_16k,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0 or peak_mode_name != 'Peak 3 Mode') 
            then raw.driver_cost_base_n_surge
        else greatest(16000,raw.unit_fee*1.2*raw.driver_distance) 
        end as opt4_16k,

        peak_mode_name
from raw 
where city_name = 'HCM City'
and order_status = 'Delivered'
)
-- where is_affect = 1 
group by 1,2,3,4
/*
select 
        city_name,
        district_name,
        weekday,
        created_date,
        start_time,end_time,
        count(distinct base.id) as gross,
        count(distinct case when base.order_status = 'Delivered' then base.id else null end) as net,
        count(distinct case when base.order_status = 'Delivered' and delivered_by = 'hub' then base.id else null end) as hub_net,
        try(count(distinct case when base.order_status = 'Delivered' then base.id else null end)*1.00/count(distinct base.id)) as g2n,
        try(count(distinct case when base.order_status != 'Delivered' and base.is_no_driver = 1 then base.id else null end)*1.00/count(distinct base.id)) as cnd,
        count(distinct case when base.order_status = 'Delivered' and is_affect = 1 and current_ship = min_fee_setting then id else null end) as net_surge_min_fee,
        count(distinct case when base.order_status = 'Delivered' and is_affect = 1 and opt1 != min_fee_setting then id else null end) as net_surge_rate_opt1,  
        count(distinct case when base.order_status = 'Delivered' and is_affect = 1 and opt2 != min_fee_setting then id else null end) as net_surge_rate_opt2,
        count(distinct case when base.order_status = 'Delivered' and is_affect = 1 and opt3 != min_fee_setting then id else null end) as net_surge_rate_opt3,
        sum(case when base.order_status = 'Delivered' then current_ship else null end) as total_shipping_fee_current,
        sum(case when base.order_status = 'Delivered' then opt1 else null end) as total_shipping_fee_opt1,
        sum(case when base.order_status = 'Delivered' then opt2 else null end) as total_shipping_fee_opt2,
        sum(case when base.order_status = 'Delivered' then opt3 else null end) as total_shipping_fee_opt3

from
(select 
        ds.city_name,
        ds.district_name,
        ds.weekday,
        ds.start_time,
        ds.end_time,
        raw.id,
        raw.driver_distance,
        raw.created_date,
        raw.delivered_by,
        cast(ds.min_fee_opt1 as bigint) as min_fee_setting,
        if((delivered_by = 'hub' or raw.group_id > 0),0,1) as is_affect,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0) then raw.driver_cost_base_n_surge
        else greatest(cast(min_fee_opt1 as bigint),raw.unit_fee*cast(surge_rate_opt1 as double)*raw.driver_distance) 
        end as opt1,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0) then raw.driver_cost_base_n_surge
        else greatest(cast(min_fee_opt1 as bigint),raw.unit_fee*cast(surge_rate_opt2 as double)*raw.driver_distance) 
        end as opt2,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0) then raw.driver_cost_base_n_surge
        else greatest(cast(min_fee_opt1 as bigint),raw.unit_fee*cast(surge_rate_opt3 as double)*raw.driver_distance) end as opt3,
        case 
        when (delivered_by = 'hub' or raw.group_id > 0) then raw.driver_cost_base_n_surge
        else dotet_total_shipping_fee        
        end as current_ship,
        raw.order_status,
        raw.is_no_driver


from driver_ops_surge_fee_ingest ds 
left join raw  
    on raw."day_of_week" = ds.weekday 
    and hour(eta_drop_time)*100+minute(eta_drop_time) between cast(ds.start_time as bigint) and cast(ds.end_time as bigint)
    and (case when raw.city_id != 219 
        then (cast(ds.district_id as bigint) = raw.district_id and cast(ds.city_id as bigint) = raw.city_id) 
        else cast(ds.city_id as bigint) = raw.city_id end)
) base 
group by 1,2,3,4,5,6
*/