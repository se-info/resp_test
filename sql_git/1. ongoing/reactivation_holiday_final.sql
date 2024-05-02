with raw as 
(select  
        report_date,
        city_name,
        shipper_tier,
        shipper_id,
        total_order_food as total_order,
        sla_rate


from driver_ops_driver_performance_tab
where 1 = 1 
and report_date between date'2024-04-28' and date'2024-05-01'
)
select sum(bonus_value) as total_bonus
-- select *
from
(select t1.*,
        case when t1.city_name not in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong') 
        and t1.sla_day_qualified >= 4 and t1.working_day_qualified >= 4 then 
        (case 
        when total_order between 48 and 79 then 90000
        when total_order > 79 then 200000 else 0 end) 

        when city_name in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong')
        and t1.working_day_qualified >= 4 then 
        (case 
        when total_order between 48 and 79 then 90000
        when total_order > 79 then 200000 else 0 end) else 0 end as bonus_value
from 
(select 
        shipper_id,
        max_by(city_name,report_date) as city_name,
        sum(total_order) as total_order,
        cardinality(array_agg(distinct case when total_order >= 4 then report_date else null end)) as working_day_qualified,
        cardinality(array_agg(distinct case when sla_rate >= 90 then report_date else null end)) as sla_day_qualified,
        map_agg(report_date,sla_rate) as sla_info
from raw
group by 1 
) t1 
inner join vnfdbi_opsndrivers.phong_test_table t2 on t1.shipper_id = cast(t2.shipper_id as bigint)
)
