with raw as 
(select 
        report_date,
        shipper_id,
        case 
        when city_name in ('HCM City','Ha Noi City') then city_name
        else 'Other' end as city_name,
        shipper_tier,
        total_order_spxi,
        online_hour,
        sla_rate,
        if(online_hour>=5,1,0) as qualified_online_hour,
        if(sla_rate>=90,1,0) as sla_qualified


from driver_ops_driver_performance_tab

where total_order_spxi > 0 
and shipper_tier != 'Hub'
and report_date between date'2024-02-01' and date'2024-02-07'
)
,f as 
(select 
        shipper_id,
        city_name,
        sum(total_order_spxi) as spxi_order,
        sum(online_hour) as online_hour,
        count(distinct case when qualified_online_hour = 1 then report_date else null end) as working_days,
        count(distinct case when sla_qualified = 1 then report_date else null end) as sla_days

from raw 

where 1=1
group by 1,2 )
select 
        f.*,
        case 
        when working_days >= 7 and sla_days >= 7 then 1 else 0 end as is_eligible,
        case 
        when working_days >= 7 and sla_days >= 7 and spxi_order between 80 and 99 then 400000
        when working_days >= 7 and sla_days >= 7 and spxi_order between 100 and 119 then 800000
        when working_days >= 7 and sla_days >= 7 and spxi_order > 119 then 1000000 else 0 end as bonus_value

from f 
-- limit 200


/*
select 
        coalesce(city_name,'VN') as cities,
        sum(spxi_order) as total_order,
        sum(bonus_value) as total_bonus,
        count(distinct shipper_id) as a7,
        count(distinct case when is_eligible = 1 then shipper_id else null end) as eligible_a7
from
(select 
        f.*,
        case 
        when working_days >= 4 and spxi_order >= 40 then 1 else 0 end as is_eligible,
        case 
        when working_days >= 4 and spxi_order >= 60 then 400000
        when working_days >= 4 and spxi_order >= 50 then 300000
        when working_days >= 4 and spxi_order >= 40 then 200000 else 0 end as bonus_value

from f )
group by grouping sets (city_name,())
*/




