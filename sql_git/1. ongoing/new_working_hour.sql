WITH temp_data(grass_date, order_id, driver_id,create_at,complete_at ) AS (
    select
            date(delivered_timestamp),id,shipper_id,last_incharge_timestamp,delivered_timestamp
from driver_ops_raw_order_tab
WHERE date(delivered_timestamp) between date'2024-07-01' and date'2024-07-31'
AND order_status = 'Delivered'
) 
,base as
(
SELECT DISTINCT 
    grass_date,
    order_id, 
    driver_id,
    create_at,
    -- FROM_UNIXTIME(o.create_time/ 1000) AS create_at
    -- ,o.create_time as create_unixtime,
    complete_at,
    -- FROM_UNIXTIME(o.delivery_complete_time/ 1000) complete_at,
    -- ,sum(balance_deposit_on_hold) over (partition by user_id order by new_txn_id ) as to_
    MAX(complete_at) OVER (PARTITION BY driver_id ORDER BY create_at rows between unbounded preceding and current row) cummax_complete_at
    
    from temp_data

)
,working_time as 
(SELECT
    grass_date, driver_id, new_grp_id, 
    MIN(create_at) min_create_at,
    MAX(complete_at) max_complete_at,
    COUNT(DISTINCT order_id) orders,
    DATE_DIFF('second',MIN(create_at),MAX(complete_at))/3600.00 as working_hour
    
FROM (
    SELECT
        grass_date, driver_id, order_id, create_at, complete_at, break_seconds, 
        MAX(new_th) OVER(PARTITION BY grass_date, driver_id ORDER BY create_at) new_grp_id

    FROM (
        SELECT 
            *,
            CASE WHEN is_groupped = 0 THEN ROW_NUMBER() OVER (PARTITION BY grass_date, driver_id, is_groupped) ELSE NULL END as new_th
        FROM ( 
        -- to get raw + understand shift
            SELECT
                grass_date, driver_id, order_id, create_at, complete_at, cummax_complete_at, shift_cummax_complete_at,  
                CASE WHEN create_at > shift_cummax_complete_at THEN (to_unixtime(create_at) - to_unixtime(shift_cummax_complete_at))/60 ELSE 0 END as break_seconds,
                CASE WHEN create_at <= shift_cummax_complete_at THEN 1 ELSE 0 END is_groupped
            FROM (
                SELECT 
                    grass_date, driver_id, order_id, create_at, complete_at, cummax_complete_at,
                    LAG(cummax_complete_at) OVER (PARTITION BY grass_date, driver_id ORDER BY cummax_complete_at) shift_cummax_complete_at
                    
                FROM base  
            )
            -- where driver_id = 40657777
            -- and grass_date = date'2024-08-12'
        )
    )
) 
GROUP BY 1, 2, 3)
select 
        date_trunc('month',grass_date) as month_,
        city_group,
        type_,
        order_range,
        sum(online_hour) as online_hour,
        sum(working_hour) as working_hour,
        COUNT(distinct (driver_id,grass_date))/count(distinct grass_date) as a1
from
(select 
        w.driver_id,
        w.grass_date,
        ds.online_hour,
        case 
        when dp.shipper_type = 12 and hub_order = 0 then '1. non hub'
        when dp.shipper_type = 12 and hub_order > 0 then '2. hub'
        else '1. non hub' end as type_,
        case when dp.city_id in (217,218,219) then dp.city_name
        else 'oth' end as city_group,
        case 
        when try(dp.total_order*1.00/ds.online_hour) <= 0.5 then '1. 0.5'
        when try(dp.total_order*1.00/ds.online_hour) <= 1 then '2. 1'
        when try(dp.total_order*1.00/ds.online_hour) <= 1.5 then '3. 1.5'
        when try(dp.total_order*1.00/ds.online_hour) <= 2 then '4. 2'
        when try(dp.total_order*1.00/ds.online_hour) <= 2.5 then '5. 2.5'
        when try(dp.total_order*1.00/ds.online_hour) <= 3 then '6. 3'
        when try(dp.total_order*1.00/ds.online_hour) <= 3.5 then '7. 3.5'
        when try(dp.total_order*1.00/ds.online_hour) <= 4 then '8. 4'
        when try(dp.total_order*1.00/ds.online_hour) <= 4.5 then '9. 4.5'
        when try(dp.total_order*1.00/ds.online_hour) <= 5 then '10. 5'
        when try(dp.total_order*1.00/ds.online_hour) > 5 then '11. ++5'
        else '1. 0.5'
        end as productivity_range,
        case 
        when dp.total_order <= 8 then '1. 0 - 8'
        when dp.total_order <= 14 then '2. 8 - 14'
        when dp.total_order <= 22 then '3. 14 - 22'
        when dp.total_order <= 30 then '4. 22 - 30'
        when dp.total_order <= 40 then '5. 30 - 40'
        when dp.total_order > 40 then '6. ++40' 
        else '1. 0 - 8'
        end as order_range,    
        SUM(w.working_hour) as working_hour

FROM working_time w 

LEFT JOIN driver_ops_driver_performance_tab dp 
    on dp.shipper_id = w.driver_id
    and dp.report_date = w.grass_date

LEFT JOIN (select created,uid,sum(online_by_hour/3600.00) as online_hour 
from driver_ops_driver_supply_tab
group by 1,2
) ds 
    ON ds.uid = w.driver_id
    AND ds.created = w.grass_date
group by 1,2,3,4,5,6,7
)
group by 1,2,3,4