with mapping as 
(select 
        d.rank_,
        sa.*,
        row_number()over(partition by sa.driver_id,date(sa.create_time) order by sa.create_time asc) as assign_rank

from driver_ops_order_assign_log_tab sa 

left join dev_vnfdbi_opsndrivers.temp_driver_list_tab d 
    on d.shipper_id = sa.driver_id

where date(sa.create_time) between date'2023-11-01' and date'2023-11-30' 
and sa.order_category != 0
and sa.status in (17,18)
and d.shipper_id is not null 
)
,metrics as 
(select 
        sa.*,
        d.total_order as current_order,
        sa.ignore_current as current_ignore,
        d.sla_rate as current_sla,
        ROUND((d.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.new_ignore,0) + d.total_order) AS DOUBLE) )*100,2) AS new_sla,
        90 as expected_sla,
        d.total_order + ROUND(sa.new_ignore*90/d.sla_rate,0) as simulation_order, 
        (COALESCE(sa.denied,0) + COALESCE(sa.new_ignore,0) + d.total_order) as total_assign

from
(SELECT 
                DATE(create_time) AS created_date
                ,driver_id
                ,COUNT(DISTINCT CASE WHEN status IN (8,9) AND TRIM(assign_type) != '6. New Stack Assign' AND order_type != 'Group' THEN (order_id,create_time) ELSE NULL END) AS ignore_current
                -- #count ignore stack/group spxi only
                ,COUNT(DISTINCT CASE WHEN (status IN (8,9,17,18) AND order_category != 0
                                           or 
                                           status IN (8,9) AND order_category = 0 AND TRIM(assign_type) != '6. New Stack Assign' AND order_type != 'Group' ) 
                                           THEN (order_id,create_time) ELSE NULL END) AS new_ignore 
                ,COUNT(DISTINCT CASE WHEN status IN (2) THEN (order_id,create_time) ELSE NULL END) AS denied

FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab 
WHERE 1 = 1 
AND status IN (2,3,8,9,17,18)
GROUP BY 1,2
) sa 

left join driver_ops_driver_performance_tab d
    on sa.driver_id = d.shipper_id
    and sa.created_date = d.report_date

inner join temp_driver_list_tab t 
    on t.shipper_id = sa.driver_id

where d.shipper_tier != 'Hub'
and d.sla_rate >= 90 
and (ROUND((d.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.new_ignore,0) + d.total_order) AS DOUBLE) )*100,2)) < 90
and sa.created_date between date'2023-11-01' and date'2023-11-30'
)
,f as 
(select 
        *,
        ROUND(current_order * 90/new_sla,0) as new_order,
        ROUND(current_order * 90/new_sla,0) - current_order as add_order

from metrics 
-- where created_date = date'2023-11-22'
)
,order_simulation as 
(select 
        ref_order_id,
        max_by(driver_id,a.rank_) as final_driver,
        max_by(a.rank_,a.rank_) as rank_
from
(select 
        f.*,
        m.rank_,
        m.order_id,
        m.ref_order_id,
        f.add_order - m.assign_rank as flag

from f 

left join mapping m 
    on m.driver_id = f.driver_id
    and date(m.create_time) = f.created_date

where f.add_order - m.assign_rank >= 0
) a 
group by 1
)
select 
        created_date,
        order_status,
        count(order_code) as non_unique,
        count(distinct order_code) as unique_order_ignore

from
(select 
        a.*,
        b.order_code,
        b.order_status,
        b.city_name,
        b.created_date

from order_simulation a 

left join driver_ops_raw_order_tab b
    on b.id = a.ref_order_id
)
group by 1,2

; -- ignore turn improvement
with mapping as 
(select 
        date_trunc('month',date(sa.create_time)) as monthly,
        sa.*,
        row_number()over(partition by sa.driver_id,date(sa.create_time) order by sa.create_time asc) as assign_rank

from driver_ops_order_assign_log_tab sa 

left join dev_vnfdbi_opsndrivers.temp_driver_list_tab d 
    on d.shipper_id = sa.driver_id
    and sa.grass_date = d.report_date

where date(sa.create_time) between date'2023-01-01' and date'2023-12-31' 
and sa.order_category != 0
and sa.status in (17,18)
and d.shipper_id is not null 
order by ref_order_id desc
)
select 
        monthly,
        city_id,
        count(distinct (driver_id,ref_order_id,create_time))/cast(count(distinct grass_date) as double) as ignore_turn
        

from mapping 
group by 1,2 
;
