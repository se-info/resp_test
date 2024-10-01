-- select * from driver_ops_spxi_recovery_scheme
with raw as 
(select 
        a.shipper_id,
        array_agg(distinct date(a.delivered_timestamp)) as working_date_info,
        count(distinct date(a.delivered_timestamp)) as working_day,
        count(distinct order_code) as cnt_order

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

inner join driver_ops_spxi_recovery_scheme i on cast(i.shipper_id as bigint) = a.shipper_id

where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered')
and date(a.delivered_timestamp) between date'2024-09-28' and date'2024-09-30'
and a.order_type != 0
group by 1 
)
select 
        dp.report_date,
        dp.shipper_id,
        dp.total_order_spxi,
        raw.working_date_info,
        raw.working_day,
        dp.sla_rate


from driver_ops_driver_performance_tab dp 

inner join driver_ops_spxi_recovery_scheme i on cast(i.shipper_id as bigint) = dp.shipper_id

left join raw on raw.shipper_id = dp.shipper_id
where dp.report_date between date'2024-09-28' and date'2024-09-30'
and dp.total_order_spxi > 0 