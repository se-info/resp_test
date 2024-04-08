WITH summary AS 
(SELECT 
         report_date
        ,shipper_id
        -- ,shipper_name
        ,city_name
        ,COUNT(DISTINCT report_date) AS working_day
        ,SUM(total_order) AS total_order
        

FROM driver_ops_driver_performance_tab raw    

where city_name = 'An Giang'
and report_date between date'2023-10-26' and date'2023-10-28'
and total_order > 0
GROUP BY 1,2,3
UNION ALL 
SELECT 
         report_date
        ,shipper_id
        -- ,shipper_name
        ,city_name
        ,COUNT(DISTINCT report_date) AS working_day
        ,SUM(total_order) AS total_order
        

FROM driver_ops_driver_performance_tab raw    

where city_name = 'Long An'
and report_date between date'2023-10-27' and date'2023-10-29'
and total_order > 0
GROUP BY 1,2,3
UNION ALL 
SELECT 
         report_date
        ,shipper_id
        -- ,shipper_name
        ,city_name
        ,COUNT(DISTINCT report_date) AS working_day
        ,SUM(total_order) AS total_order
        

FROM driver_ops_driver_performance_tab raw    

where city_name = 'Tien Giang'
and report_date between date'2023-10-25' and date'2023-10-27'
and total_order > 0
GROUP BY 1,2,3
)
select 
        s1.report_date,
        s1.shipper_id,
        s1.city_name,
        s1.total_order,
        s2.working_day,
        case 
        when s2.working_day >= 3 and s1.total_order >= 10 then 50000
        else 0 end as bonus_value

from summary s1 

left join (select shipper_id,count(distinct report_date) as working_day from summary group by 1 ) s2
    on s1.shipper_id = s2.shipper_id



