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
and city_name in ('HCM City','Ha Noi City')
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
,m as
(select 
        f.*,
        sm.shipper_name,
        case 
        when working_days >= 7 and sla_days >= 7 then 1 else 0 end as is_eligible,
        case 
        when working_days >= 7 and sla_days >= 7 and spxi_order between 80 and 99 then 400000
        when working_days >= 7 and sla_days >= 7 and spxi_order between 100 and 119 then 800000
        when working_days >= 7 and sla_days >= 7 and spxi_order > 119 then 1000000 else 0 end as bonus_value

from f 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = f.shipper_id and sm.grass_date = 'current'
)
,order_raw as 
(select 
        id as ref_id,
        shipper_id,
        city_name,
        row_number()over(partition by shipper_id order by created_timestamp asc) as rank_,
        'Thuong don SPX '||date_format(date(delivered_timestamp),'%d/%m/%Y')||' '||cast(id as varchar) as note



from driver_ops_raw_order_tab
where order_type = 6
and order_status = 'Delivered'
and city_name in ('HCM City','Ha Noi City')
and date(delivered_timestamp) between date'2024-02-01' and date'2024-02-07'
)
select 
        o.*,
        m.shipper_name,
        m.bonus_value*1.00/10 as bonus_value

from order_raw o 

inner join (select * from m where bonus_value > 0) m 
        on m.shipper_id = o.shipper_id

where o.rank_ <= 10









