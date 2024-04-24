with raw as 
(select  
        report_date,
        city_name,
        shipper_tier,
        shipper_id,
        total_order,
        sla_rate


from driver_ops_driver_performance_tab
where 1 = 1 
and total_order > 0 
and shipper_tier != 'Hub'
and report_date between date'2023-12-30' and date'2024-01-02'
and (shipper_tier in (case 
when city_name  not in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong') then 'Level 1'
end
)
or shipper_tier in (case 
when city_name in ('HCM City','Ha Noi City') then 'Level 2' 
end)
or city_name in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong')
)
)
select  
        t1.*,
        t2.*


from raw t1 

left join  
(select 
        shipper_id,
        cardinality(array_agg(distinct report_date)) as working_day,
        cardinality(array_agg(distinct case when total_order >= 6 then report_date else null end)) as working_day_qualified,
        cardinality(array_agg(distinct case when sla_rate >= 90 then report_date else null end)) as sla_day_qualified,
        map_agg(report_date,sla_rate) as sla_info
from raw
group by 1 
) t2 on t2.shipper_id = t1.shipper_id