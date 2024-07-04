with raw as 
(select  
        date_trunc('month',dp.report_date) as month_,
        dp.report_date,
        dp.shipper_id,
        dp.city_name,
        case 
        when city_name in ('HCM City','Ha Noi City','Da Nang City') then city_name
        else 'Other' end as cities,
        substr(cast(sp.birth_date as varchar),1,4) as year_of_birth,
        YEAR(dp.report_date) - CAST(substr(cast(sp.birth_date as varchar),1,4) as bigint) as year_old,
        online_hour

from driver_ops_driver_performance_tab dp 
left join shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live sp 
        on sp.uid = dp.shipper_id

where dp.total_order > 0 
and regexp_like(city_name,'Dien Bien|Test|Stress|test|stress') = false
)
,f as
(select 
        month_,
        report_date,
        cities,
        case 
        when year_old between 18 and 22 then '1. 18 - 22'
        when year_old between 23 and 27 then '2. 23 - 27'
        when year_old between 28 and 32 then '3. 28 - 32'
        when year_old between 33 and 37 then '4. 33 - 37'
        when year_old between 38 and 42 then '5. 38 - 42'
        when year_old between 43 and 47 then '6. 43 - 47'
        when year_old between 48 and 52 then '7. 48 - 52'
        when year_old between 53 and 58 then '8. 53 - 58'
        when year_old > 58 then '9. ++58' 
        else '10. Unknow' end as year_old_range,
        sum(online_hour)/cast(count(distinct shipper_id) as double) as avg_supply_hour
from raw 

group by 1,2,3,4
)
select 
        month_,
        coalesce(cities,'VN') as cities,
        coalesce(year_old_range,'Overall') as year_old_range,
        avg(avg_supply_hour) as avg_supply_hour

from f 
where month_ >= date'2023-04-01'
and month_ <= date'2023-12-01'
group by 1,grouping sets((cities,year_old_range),(cities),(year_old_range),())


