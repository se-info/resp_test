select sum(coalesce(adjustment,0)) as total_fee
from
(select
        raw.id as ref_id,
        raw.shipper_id,
        raw.city_name,
        raw.created_date,
        date(raw.delivered_timestamp) as report_date,
        raw.delivered_timestamp,
        coalesce(osl.last_auto_assign_time,raw.created_timestamp) as last_incharge_timestamp,
        raw.first_auto_assign_timestamp,
        raw.late_night_service_fee,
        raw.holiday_service_fee,
        raw.holiday_service_fee + 13500 as shipping_received,
        mf.min_fee as shipping_fee_expected,
        case 
        when (cast(mf.min_fee as bigint) - (raw.holiday_service_fee + 13500)) < 0 then 0 
        else (cast(mf.min_fee as bigint) - (raw.holiday_service_fee + 13500))  end as adjustment,
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

left join dev_vnfdbi_opsndrivers.hub_sunday_min_fee_ingest mf 
    on cast(mf.city_id as bigint) = raw.city_id
    and mf.week_day = date_format(coalesce(osl.last_auto_assign_time,raw.created_timestamp),'%a')
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) >= cast(mf.start_time as bigint) 
    and (HOUR(coalesce(osl.last_auto_assign_time,raw.created_timestamp))*100 + MINUTE(coalesce(osl.last_auto_assign_time,raw.created_timestamp))) < cast(mf.end_time as bigint)

where raw.driver_policy = 2
and raw.order_type = 0
and raw.order_status = 'Delivered'
and date(coalesce(osl.last_auto_assign_time,raw.created_timestamp)) = date'2024-05-12'
and late_night_service_fee = 0
)
where date(delivered_timestamp) = autopay_date_ts