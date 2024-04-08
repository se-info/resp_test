with deny_tab as
(select 
    uid as shipper_id
    ,order_id
    ,from_unixtime(create_time-3600) as deny_time
    ,lower(reason_text) as reason
from shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live
where lower(reason_text) like '%đơn hàng không thuộc khu vực hoạt động%'
and from_unixtime(create_time-3600) >= date_add('hour',17,cast(date '2022-05-23' as timestamp)) and from_unixtime(create_time-3600) < date_add('hour',12,cast(date '2022-05-24' as timestamp))
-- limit 10
)

,hub_base as
(select 
    hub.report_date
    ,hub.shipper_id
    ,hub.shipper_name
    ,hub.city_name
    ,hub.hub_type_v2
    ,shift.start_shift_time
    ,shift.end_shift_time
    ,date_add('hour',cast(shift.start_shift_time as bigint),cast(hub.report_date as timestamp )) as start_shift
    ,date_add('hour',cast(shift.end_shift_time as bigint),cast(hub.report_date as timestamp )) as end_shift
from vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab hub
left join dev_vnfdbi_opsndrivers.shopeefood_vn_driver_hub_shift_tracking shift
    on hub.report_date = shift.report_date and hub.shipper_id = shift.shipper_id
where  hub.report_date between date  '2022-05-23' and date '2022-05-24'
)
select 
    hub.*
    ,deny.*
from hub_base hub
inner join deny_tab deny
    on hub.shipper_id = deny.shipper_id and deny_time between hub.start_shift and hub.end_shift
