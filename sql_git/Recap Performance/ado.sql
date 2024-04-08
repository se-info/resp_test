select 
    date_trunc('month',report_date) as report_month
    ,count(distinct (o.shipper_id,report_date)) / day(date_trunc('month',report_date) + interval '1' month - interval '1' day) as trans_drivers
    ,count(distinct case when is_order_in_hub_shift = 1 then (o.shipper_id,report_date) else null end) / day(date_trunc('month',report_date) + interval '1' month - interval '1' day) as avg_hub_drivers
    ,1.0000*count(distinct case when is_order_in_hub_shift = 1 then uid else null end) / count(distinct case when is_order_in_hub_shift = 1 then (o.shipper_id,report_date) else null end) as net_hub_ado
    ,1.0000*count(distinct case when is_order_in_hub_shift != 1 then uid else null end) / (count(distinct (shipper_id,report_date)) - count(distinct case when is_order_in_hub_shift = 1 then (o.shipper_id,report_date) else null end)) as net_non_hub_ado
    ,1.0000*cast(count(distinct uid) as double) / count(distinct (shipper_id,report_date)) as net_ado

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o
                                                             
                                                                                          
where report_date between date '2022-12-01' and date '2023-04-30'
and order_status = 'Delivered'
and city_name in ('HCM City','Ha Noi City')                         
                            
group by 1