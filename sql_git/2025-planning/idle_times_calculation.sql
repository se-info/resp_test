WITH temp_data(grass_date, order_id,order_code,group_id, driver_id,create_at,complete_at ) AS (
select 
        date(from_unixtime(r.accept_time - 3600)),
        r.ref_order_id,
        r.ref_order_code,
        r.group_id,
        uid,
        from_unixtime(r.accept_time - 3600),
        case 
        when order_status = 400 then from_unixtime(r.real_drop_time - 3600)
        when order_status = 401 then from_unixtime(r.quit_time - 3600)
        when order_status = 405 then from_unixtime(r.complete_time - 3600)
        end
        -- order_status,
        -- order_code

from shopeefood.foody_partner_db__driver_order_tab__reg_continuous_s0_live r 
where 1 = 1
and date(from_unixtime(r.accept_time - 3600)) between date'2024-09-01' and date'2024-10-16'
and order_status IN (400,401,405)
) 
,base as
(SELECT DISTINCT 
    grass_date,
    order_id,
    order_code, 
    group_id,
    driver_id,
    create_at,
    complete_at,
    MAX(complete_at) OVER (PARTITION BY driver_id ORDER BY create_at rows between unbounded preceding and current row) cummax_complete_at
    
from temp_data
)
,s as 
(select 
        *,
        CASE WHEN is_groupped = 0 THEN ROW_NUMBER() OVER (PARTITION BY grass_date, driver_id, is_groupped) ELSE NULL END as new_th
from
(SELECT 
        grass_date, 
        driver_id, 
        order_id,
        order_code,
        group_id, 
        create_at, 
        complete_at, 
        cummax_complete_at,
        LAG(cummax_complete_at) OVER (PARTITION BY grass_date, driver_id ORDER BY cummax_complete_at) shift_cummax_complete_at,
        CASE WHEN create_at <= LAG(cummax_complete_at) OVER (PARTITION BY grass_date, driver_id ORDER BY cummax_complete_at) THEN 1 ELSE 0 END is_groupped
        
FROM base  
)
)
,final_ as 
(select 
        grass_date,
        group_cal,
        driver_id,
        min(create_at) as min_created,
        max(complete_at) as max_completed,
        date_diff('second',min(create_at),max(complete_at))*1.0000/3600 as working_hour

from
(select
        *,
        MAX(new_th) OVER(PARTITION BY grass_date, driver_id ORDER BY create_at) group_cal
from s 
)
group by 1,2,3
)
,b as 
(select 
        f.*,
        dp.online_hour,
        dp.city_name
from
(select 
        f.grass_date,
        f.driver_id,
        sum(working_hour) as working_hour


from final_ f 

group by 1,2
) f 
left join driver_ops_driver_performance_tab dp on dp.shipper_id = f.driver_id and dp.report_date = f.grass_date and dp.total_order > 0 

)
select 
        grass_date,
        coalesce(city_name,'VN') as city_name,
        sum(working_hour)*1.0000/sum(online_hour) -1 as down_time
from b
where online_hour > 0
group by 1,grouping sets(city_name,())