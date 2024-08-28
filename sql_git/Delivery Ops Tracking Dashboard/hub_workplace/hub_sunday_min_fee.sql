with min_fee_tab(report_date,start_time,end_time,city_id,min_fee) as 
(VALUES
(date'2024-08-25',1700,2000,217,16000),
(date'2024-08-25',1030,1230,218,15000),
(date'2024-08-25',1700,2000,218,16000)
)
,raw as 
(select
        raw.group_id,
        raw.id as ref_id,
        raw.shipper_id,
        raw.city_name,
        raw.created_date,
        date(raw.delivered_timestamp) as report_date,
        coalesce(osl.last_auto_assign_time,raw.created_timestamp) as last_incharge_timestamp,
        raw.first_auto_assign_timestamp,
        raw.late_night_service_fee,
        raw.holiday_service_fee,
        raw.holiday_service_fee + 13500 as shipping_received,
        mf.min_fee as shipping_fee_expected,
        date(from_unixtime(d.autopay_date_ts -3600)) as autopay_date_ts,
        (HOUR(osl.last_auto_assign_time)*100 + MINUTE(osl.last_auto_assign_time)) as hm,
        (ogi.ship_fee*1.00/100)/2 as group_fee,
        row_number()over(partition by raw.group_id order by raw.id desc) as rank_ 



from driver_ops_raw_order_tab raw

left join 
(select 
        order_id,
        from_unixtime(coalesce(max(case when status = 21 then create_time else null end),0) - 3600) as last_auto_assign_time 

from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
group by 1) osl on osl.order_id = raw.id

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live d 
    on d.ref_order_id = raw.id

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi 
        on ogi.id = raw.group_id 

left join min_fee_tab mf 
    on mf.city_id = raw.city_id
    and mf.report_date = date(raw.last_incharge_timestamp)
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) >= mf.start_time 
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) < mf.end_time

where 1 = 1 
and d.ref_order_id is not null 
and raw.order_type = 0
and raw.order_status = 'Delivered'
and date(coalesce(osl.last_auto_assign_time,raw.created_timestamp)) = date'2024-08-25'
)
select sum(adjustment)
from
(select  
        *,
        case 
        when (shipping_fee_expected - (raw.holiday_service_fee + 13500)) < 0 then 0 
        when group_id > 0 then (shipping_fee_expected - (raw.holiday_service_fee + group_fee))
        else (shipping_fee_expected - (raw.holiday_service_fee + 13500)) end as adjustment

from raw 
where shipping_fee_expected is not null )