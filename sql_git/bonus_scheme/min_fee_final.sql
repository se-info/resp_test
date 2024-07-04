with min_fee_tab(report_date,start_time,end_time,city_id,min_fee) as 
(VALUES
(date'2024-03-03',600,1029,217,13500),
(date'2024-03-03',1000,1230,217,16000),
(date'2024-03-03',1231,1659,217,13500),
(date'2024-03-03',1700,2000,217,17000),
(date'2024-03-03',2001,2200,217,13500),
(date'2024-03-03',600,1029,218,13500),
(date'2024-03-03',1000,1230,218,16000),
(date'2024-03-03',1231,1659,218,13500),
(date'2024-03-03',1700,2000,218,17000),
(date'2024-03-03',2001,2200,218,13500)
)
-- # Replace logic last_incharged_timestamp to last_auto_assign_time
select sum(adjustment) from
(select
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
        case 
        when (mf.min_fee - (raw.holiday_service_fee + 13500)) < 0 then 0 
        else (mf.min_fee - (raw.holiday_service_fee + 13500))  end as adjustment,
        date(from_unixtime(d.autopay_date_ts -3600)) as autopay_date_ts,
        (HOUR(osl.last_auto_assign_time)*100 + MINUTE(osl.last_auto_assign_time)) as hm 



from driver_ops_raw_order_tab raw

left join 
(select 
        order_id,
        from_unixtime(coalesce(max(case when status = 21 then create_time else null end),0) - 3600) as last_auto_assign_time 

from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
group by 1) osl on osl.order_id = raw.id

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live d 
    on d.ref_order_id = raw.id

left join min_fee_tab mf 
    on mf.city_id = raw.city_id
    and mf.report_date = date(raw.last_incharge_timestamp)
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) >= mf.start_time 
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) < mf.end_time

where raw.driver_policy = 2
and raw.order_type = 0
and raw.source = 'order_food'
and raw.order_status = 'Delivered'
and date(coalesce(osl.last_auto_assign_time,raw.created_timestamp)) = date'2024-03-03'
and late_night_service_fee = 0
)