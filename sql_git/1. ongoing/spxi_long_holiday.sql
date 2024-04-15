with raw as 
(select 
        report_date,
        shipper_id,
        case 
        when city_name in ('HCM City','Ha Noi City') then city_name
        else 'Other' end as city_name,
        shipper_tier,
        ROUND(total_order_spxi*1.1,0) as total_order_spxi,
        round(greatest(online_hour,work_hour) *1.1,0) as online_hour,
        sla_rate,
        if(round(greatest(online_hour,work_hour)*1.1,0)>=8,1,0) as qualified_online_hour,
        if(sla_rate>=1,1,0) as sla_qualified


from driver_ops_driver_performance_tab

where total_order_spxi > 0 
and shipper_tier != 'Hub'
and report_date between date'2024-04-04' and date'2024-04-08'
)
,f as 
(select 
        shipper_id,
        city_name,
        sum(total_order_spxi) as spxi_order,
        sum(online_hour) as online_hour,
        count(distinct report_date) as working_days_original,
        count(distinct case when qualified_online_hour = 1 then report_date else null end) as working_days,
        count(distinct case when sla_qualified = 1 then report_date else null end) as sla_days

from raw 

where 1=1
group by 1,2 )
select 
        f.*,
        case 
        when working_days >= 5 and sla_days >= 5 then 1 else 0 end as is_eligible,
        case 
        when working_days >= 5 and sla_days >= 5 and spxi_order between 80 and 99 then 150000
        when working_days >= 5 and sla_days >= 5 and spxi_order between 100 and 119 then 250000
        when working_days >= 5 and sla_days >= 5 and spxi_order > 119 then 400000 else 0 end as bonus_value,
        ag.district_name_agg

from f 

left join 
(select 
        shipper_id,
        array_agg(distinct di.name_en) as district_name_agg

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = a.district_id
where a.order_status = 'Delivered'
and date(a.delivered_timestamp) between date'2024-04-04' and date'2024-04-08'
group by 1 
) ag on ag.shipper_id = f.shipper_id
