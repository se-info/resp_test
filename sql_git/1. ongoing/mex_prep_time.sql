with city_filter as 
(select 
        case 
        when city_name in ('HCM City','Ha Noi City','Da Nang City') then city_name
        else 'Other' end as cities,
        avg(case when avg_prepared_time > 0 then avg_prepared_time else null end) as avg_prepared_time

from dev_vnfdbi_opsndrivers.driver_ops_merchant_tracking_tab
where created_date between current_date - interval '60' day and current_date - interval '1' day 

group by 1
)
select *

from
(select 
        v1.merchant_id,
        v1.merchant_name,
        v1.city_name,
        c.avg_prepared_time as avg_prepared_time_city,
        sum(v1.net) as net_order,
        sum(case when v1.avg_prepared_time > 0 then v1.avg_prepared_time else null end)/count(distinct created_date) as avg_prepared_time

from dev_vnfdbi_opsndrivers.driver_ops_merchant_tracking_tab v1 
 
left join city_filter c on c.cities = v1.city_name

where created_date between current_date - interval '30' day and current_date - interval '1' day
and city_name in ('HCM City','Ha Noi City','Da Nang City')

group by 1,2,3,4
)
where net_order >= 60
and avg_prepared_time > avg_prepared_time_city
