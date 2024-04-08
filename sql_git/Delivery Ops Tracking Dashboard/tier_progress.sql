with raw as 
(select 
        date_trunc('month',report_date) as month_,
        date_trunc('month',report_date) + interval '1' month as next_month,
        shipper_id,
        city_name,
        sum(total_order) as total_order,
        min_by(shipper_tier,report_date) as tier_at_first_date



from driver_ops_driver_performance_tab
where 1 = 1 
and regexp_like(lower(city_name),'test|dien bien|stress') = false 
group by 1,2,3,4
)
,f as 
(select 
        t1.*,
        t2.tier_at_first_date as tier_at_first_date_next_month


from raw t1 
left join raw t2 
        on t2.shipper_id = t1.shipper_id 
        and t2.month_ = t1.next_month
where t1.month_ between date'2023-08-01' and date'2023-11-01'
)
-- select * from f where tier_at_first_date_next_month is null
select 
        f.month_ as current_month,
        f.next_month,
        f.city_name,
        f.tier_at_first_date,
        coalesce(f.tier_at_first_date_next_month,'quit_work') as tier_at_first_date_next_month,
        count(distinct shipper_id) as total_driver,
        count(distinct case when total_order > 0 then shipper_id else null end) as driver_have_active_at_current_month

from f 

group by 1,2,3,4,5
