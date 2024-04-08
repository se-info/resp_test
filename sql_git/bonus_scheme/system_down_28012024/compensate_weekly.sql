WITH params(report_date,uid,hub_type_x_start_time) AS
(VALUES
(date'2023-03-24',01010101,'10 hour shift-10')
) 
,online_check as 
(select
        created,
        uid,
        sum(h11_online_time) as h11
from 
(select 
         uid
        ,date(from_unixtime(create_time - 3600)) as created
        ,from_unixtime(checkin_time - 3600) as checkin
        ,from_unixtime(checkout_time - 3600) as checkout  
        ,cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour as h11_start
        ,cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour as h11_end
        ,case 
        when from_unixtime(checkout_time - 3600) < cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour then 0
        when from_unixtime(checkin_time - 3600) > cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour then 0
        else date_diff('second',
            greatest(cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour,from_unixtime(checkin_time - 3600)),   
            least(cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour,from_unixtime(checkout_time - 3600))   
            )*1.0000/(60*60)
        end as h11_online_time


from shopeefood.foody_partner_db__shipper_checkin_checkout_log_tab__reg_daily_s0_live 
where 1 = 1 
and date(from_unixtime(create_time - 3600)) = date'2024-01-28'
)
group by 1,2
)
,check_qualified AS
(SELECT 
        YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
       ,a.uid AS shipper_id 
       ,SUM(a.registered_) AS total_reg
       ,COUNT(DISTINCT CASE WHEN (total_order > 0 or total_income > 0) then (a.date_,a.slot_id) ELSE NULL END) AS working_day 
       ,CASE 
            WHEN SUM(a.registered_) = COUNT(DISTINCT CASE WHEN (total_order > 0 or total_income > 0) then (a.date_,a.slot_id) ELSE NULL END) THEN 1 
            ELSE 0 END AS is_qualified
FROM dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab a 
WHERE 1 = 1
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
GROUP BY 1,2
)
,check_qualified_sun AS
(SELECT 
        YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
       ,a.uid AS shipper_id 
       ,SUM(a.registered_) AS total_reg
       ,COUNT(DISTINCT CASE WHEN (total_order > 0 or total_income > 0) then (a.date_,a.slot_id) ELSE NULL END) AS working_day 
       ,CASE 
            WHEN SUM(a.registered_) = COUNT(DISTINCT CASE WHEN (total_order > 0 or total_income > 0) then (a.date_,a.slot_id) ELSE NULL END) THEN 1 
            ELSE 0 END AS is_qualified
FROM dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab a 
WHERE 1 = 1
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND date_format(a.date_,'%a') = 'Sun'
GROUP BY 1,2
)
,metrics_weekly AS 
(SELECT
       YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
      ,a.uid as shipper_id
      ,a.hub_type_original AS hub_type
      ,cq.is_qualified  
      ,MAX_BY(a.shipper_name,YEAR(a.date_)*100 + WEEK(a.date_)) AS shipper_name
      ,MAX_BY(a.city_name,YEAR(a.date_)*100 + WEEK(a.date_))city_name
      ,SUM(a.registered_) AS total_reg
      ,COUNT(DISTINCT CASE WHEN (total_order > 0 or total_income > 0) then (a.date_,a.slot_id) ELSE NULL END) AS working_day  
      ,SUM(final_kpi) AS kpi_original
      ,SUM(kpi_adjusted) AS kpi_adjusted
      ,SUM(CASE WHEN date_format(a.date_,'%a') = 'Sun' THEN a.kpi ELSE 0 END) AS kpi_sun
      ,SUM(case when date_format(a.date_,'%a') = 'Sun' then a.total_order else 0 end) as is_work_sun         
      ,SUM(total_order) AS total_order  
FROM 
(SELECT a.*
       ,CASE 
             WHEN hub_type_original = '3 hour shift' then 0
             ELSE a.kpi END AS final_kpi    
       ,CASE 
        WHEN p.uid is not null then 1
        WHEN a.hub_type_original = '3 hour shift' then 0
        WHEN a.date_ = date'2024-01-28' and (HOUR(start_shift_time) between 16 and 20 OR HOUR(end_shift_time) between 16 and 20) 
        and a.hub_type_original in ('10 hour shift','8 hour shift') and a.online_peak_hour >= 1 
                and total_order > 0 and ac.no_ignored = 0 and ac.no_deny = 0 then 1 
        WHEN a.date_ = date'2024-01-28' and (HOUR(start_shift_time) between 16 and 20 OR HOUR(end_shift_time) between 16 and 20) 
        and a.hub_type_original not in ('10 hour shift','8 hour shift') 
                and total_order > 0 and ac.no_ignored = 0 and ac.no_deny = 0 then 1 
             ELSE a.kpi END AS kpi_adjusted                  
FROM dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab a 


left join online_check oc 
    on oc.uid = a.uid 
    and oc.created = a.date_  

LEFT JOIN params p 
    on p.uid = a.uid 
    and p.report_date = a.date_
    and p.hub_type_x_start_time = a.hub_type_x_start_time


left join 
(select 
         date(sa.create_time) as created
        ,sa.driver_id
        ,h.hub_type_x_start_time as hub_type_original_ac 
        ,array_agg(distinct case when status in (8,9,17,18,2,14,15) then order_code else null end) as order_info
        ,count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_assign
        ,count(distinct case when status in (3,4) then (driver_id,order_code,create_time) else null end) as no_incharged
        ,count(distinct case when status in (8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_ignored
        ,count(distinct case when status in (2,14,15) then (driver_id,order_code,create_time) else null end) as no_deny

from driver_ops_order_assign_log_tab sa  

left join (select * from driver_ops_hub_driver_performance_tab where registered_ = 1) h 
       on sa.driver_id = h.uid 
       and date(sa.create_time) = h.date_
       and sa.create_time between h.start_shift_time and h.end_shift_time

where date(create_time) = date'2024-01-28' 
and hour(create_time) >= 10 
and hour(create_time) <= 15
and status in (3,4,2,14,15,8,9,17,18) 
and h.hub_type_original is not null
group by 1,2,3
) ac on ac.driver_id = a.uid and ac.created = a.date_ and ac.hub_type_original_ac = a.hub_type_x_start_time
                
)a

LEFT JOIN check_qualified cq 
    on cq.created_year_week = (YEAR(a.date_)*100 + WEEK(a.date_))
    and cq.shipper_id = a.uid

WHERE 1 = 1                                            
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND registered_ = 1 
GROUP BY 1,2,3,4
)
,metrics_sunday AS 
(SELECT
       YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type_original AS hub_type
      ,cqs.is_qualified AS is_qualified_sun
      ,COUNT(a.slot_id) AS registered_slot
      ,SUM(a.final_kpi) AS kpi_original
      ,SUM(a.kpi_adjusted) AS kpi_adjusted 
      ,COUNT(CASE WHEN (total_order > 0 or total_income > 0) THEN a.slot_id ELSE NULL END) AS working_slot        

FROM
(SELECT a.*
       ,a.kpi AS final_kpi    
       ,CASE 
        WHEN p.uid is not null then 1
        WHEN a.date_ = date'2024-01-28' and (HOUR(start_shift_time) between 16 and 20 OR HOUR(end_shift_time) between 16 and 20) 
        and a.hub_type_original in ('10 hour shift','8 hour shift') and a.online_peak_hour >= 1 
                and total_order > 0 and ac.no_ignored = 0 and ac.no_deny = 0 then 1 
        WHEN a.date_ = date'2024-01-28' and (HOUR(start_shift_time) between 16 and 20 OR HOUR(end_shift_time) between 16 and 20) 
        and a.hub_type_original not in ('10 hour shift','8 hour shift') 
                and total_order > 0 and ac.no_ignored = 0 and ac.no_deny = 0 then 1 
             ELSE a.kpi END AS kpi_adjusted              
FROM dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab a 

left join 
(select 
         date(sa.create_time) as created
        ,sa.driver_id
        ,h.hub_type_x_start_time as hub_type_original_ac
        ,array_agg(distinct case when status in (8,9,17,18,2,14,15) then order_code else null end) as order_info
        ,count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_assign
        ,count(distinct case when status in (3,4) then (driver_id,order_code,create_time) else null end) as no_incharged
        ,count(distinct case when status in (8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_ignored
        ,count(distinct case when status in (2,14,15) then (driver_id,order_code,create_time) else null end) as no_deny

from driver_ops_order_assign_log_tab sa  

left join (select * from driver_ops_hub_driver_performance_tab where registered_ = 1) h 
       on sa.driver_id = h.uid 
       and date(sa.create_time) = h.date_
       and sa.create_time between h.start_shift_time and h.end_shift_time

where date(create_time) = date'2024-01-28' 
and hour(create_time) >= 10 
and hour(create_time) <= 15
and status in (3,4,2,14,15,8,9,17,18) 
and h.hub_type_original is not null
group by 1,2,3
) ac on ac.driver_id = a.uid and ac.created = a.date_ and ac.hub_type_original_ac = a.hub_type_x_start_time

LEFT JOIN params p 
    on p.uid = a.uid 
    and p.report_date = a.date_
    and p.hub_type_x_start_time = a.hub_type_x_start_time                
)a

LEFT JOIN check_qualified_sun cqs
    on cqs.created_year_week = (YEAR(a.date_)*100 + WEEK(a.date_))    
    and cqs.shipper_id = a.uid
    
WHERE 1 = 1                                            
AND a.date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND date_format(a.date_,'%a') = 'Sun'
AND a.registered_ = 1
AND a.city_name in ('HCM City','Ha Noi City')
GROUP BY 1,2,3,4,5,6
)
,final_sunday AS 
(SELECT
         created_year_week
        ,shipper_id
        ,SUM(sunday_bonus_original) AS sunday_bonus_original
        ,SUM(sunday_bonus_adjusted) AS sunday_bonus_adjusted
        ,MAP_AGG(hub_type,sunday_bonus_adjusted) AS sunday_bonus_ext
        ,MAP_AGG(hub_type,kpi_adjusted) AS sunday_bonus_x_kpi_adjusted
FROM
(SELECT
         created_year_week
        ,shipper_id
        ,shipper_name 
        ,city_name
        ,hub_type
        ,is_qualified_sun
        ,kpi_adjusted
        ,kpi_original
        ,CASE 
            WHEN hub_type in ('8 hour shift','10 hour shift') AND is_qualified_sun = 1 AND kpi_original >= 1 then 50000
            WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND kpi_original >= 2 then 50000
            WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND kpi_original >= 1 then 30000
            WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND kpi_original >= 2 then 30000
            WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND kpi_original >= 1 then 20000
            ELSE 0 END AS sunday_bonus_original        
        ,CASE 
            WHEN hub_type in ('8 hour shift','10 hour shift') AND is_qualified_sun = 1 AND kpi_adjusted >= 1 then 50000
            WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND kpi_adjusted >= 2 then 50000
            WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND kpi_adjusted >= 1 then 30000
            WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND kpi_adjusted >= 2 then 30000
            WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND kpi_adjusted >= 1 then 20000
            ELSE 0 END AS sunday_bonus_adjusted
FROM metrics_sunday )
GROUP BY 1,2
)
SELECT 
        coalesce(city_name,'vn') as cities,
        sum(weekly_bonus_orginal) as week_original,
        sum(sunday_bonus_original) as sun_original,
        sum(weekly_bonus_adjusted) as week_,
        sum(sunday_bonus_adjusted) as sun_,
        count(distinct case when weekly_bonus_adjusted != weekly_bonus_orginal then shipper_id else null end) as driver_adjusted_weekly,
        count(distinct case when sunday_bonus_adjusted != sunday_bonus_original then shipper_id else null end) as driver_adjusted_sunday 

-- select *

FROM
(SELECT  
       mw.created_year_week
      ,mw.shipper_id 
      ,MAX_BY(mw.shipper_name,mw.created_year_week) AS shipper_name 
      ,MAX_BY(mw.city_name,mw.created_year_week) AS city_name
      ,MAX(mw.is_qualified) AS is_qualified_active_day
      ,SUM(mw.total_reg) AS total_registered
      ,SUM(mw.working_day) AS total_working_day
      ,SUM(mw.kpi_original) AS total_kpi_original
      ,SUM(mw.kpi_adjusted) AS total_kpi_adjusted
      ,SUM(mw.weekly_bonus_orginal) AS weekly_bonus_orginal
      ,SUM(mw.weekly_bonus_adjusted) AS weekly_bonus_adjusted
      ,MAP_AGG(mw.hub_type,mw.weekly_bonus_adjusted) AS weekly_bonus_ext
      ,MAP_AGG(mw.hub_type,mw.kpi_adjusted) AS weekly_bonus_x_kpi_adjusted
      ,COALESCE(ms.sunday_bonus_adjusted,0) AS sunday_bonus_adjusted
      ,COALESCE(ms.sunday_bonus_original,0) AS sunday_bonus_original    
      ,ms.sunday_bonus_ext
      ,ms.sunday_bonus_x_kpi_adjusted
      ,COALESCE(cp.weekly_paid,0) AS weekly_paid
      ,COALESCE(cp.sunday_paid,0) AS sunday_paid       
      ,COALESCE(SUM(mw.total_order),0) AS total_order

FROM 
(SELECT 
       mw.created_year_week
      ,mw.shipper_id
      ,mw.shipper_name
      ,mw.city_name
      ,mw.hub_type
      ,CASE
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 6 AND mw.is_qualified = 1 THEN 300000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 5 AND mw.is_qualified = 1 THEN 150000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 4 AND mw.is_qualified = 1 THEN 100000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name = 'Hai Phong City' AND mw.kpi_original >= 6 AND mw.is_qualified = 1 THEN 200000            
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name = 'Hai Phong City' AND mw.kpi_original >= 4 AND mw.is_qualified = 1 THEN 80000

            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 12 AND mw.is_qualified = 1 THEN 500000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 9 AND mw.is_qualified = 1 THEN 300000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_original >= 6 AND mw.is_qualified = 1 THEN 150000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name = 'Hai Phong City' AND mw.kpi_original >= 9 AND mw.is_qualified = 1 THEN 200000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name = 'Hai Phong City' AND mw.kpi_original >= 6 AND mw.is_qualified = 1 THEN 80000
            ELSE 0 END AS weekly_bonus_orginal
      ,CASE
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 6 AND mw.is_qualified = 1 THEN 300000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 5 AND mw.is_qualified = 1 THEN 150000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 4 AND mw.is_qualified = 1 THEN 100000
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name = 'Hai Phong City' AND mw.kpi_adjusted >= 6 AND mw.is_qualified = 1 THEN 200000            
            WHEN mw.hub_type in ('8 hour shift','10 hour shift') AND mw.city_name = 'Hai Phong City' AND mw.kpi_adjusted >= 4 AND mw.is_qualified = 1 THEN 80000

            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 12 AND mw.is_qualified = 1 THEN 500000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 9 AND mw.is_qualified = 1 THEN 300000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name in ('HCM City','Ha Noi City') AND mw.kpi_adjusted >= 6 AND mw.is_qualified = 1 THEN 150000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name = 'Hai Phong City' AND mw.kpi_adjusted >= 9 AND mw.is_qualified = 1 THEN 200000
            WHEN mw.hub_type = '5 hour shift' AND mw.city_name = 'Hai Phong City' AND mw.kpi_adjusted >= 6 AND mw.is_qualified = 1 THEN 80000
            ELSE 0 END AS weekly_bonus_adjusted
      ,mw.total_reg
      ,mw.working_day
      ,mw.kpi_original
      ,mw.kpi_adjusted
      ,mw.is_qualified
      ,total_order

FROM metrics_weekly mw
) mw

LEFT JOIN final_sunday ms 
    on ms.created_year_week = mw.created_year_week
    and ms.shipper_id = mw.shipper_id

LEFT JOIN (SELECT      
                  user_id
                --  ,note
                 ,SUM(CASE WHEN txn_type = 505 THEN balance/cast(100 as double) ELSE NULL END) as weekly_paid   
                 ,SUM(CASE WHEN txn_type = 520 THEN balance/cast(100 as double) ELSE NULL END) as sunday_paid   



FROM 
(
    select 
            user_id,
            txn_type,
            balance,
            case 
            when note = 'HUB_MODEL_Thuong tai xe guong mau tuan 11/12 - 17/11' then 'HUB_MODEL_Thuong tai xe guong mau tuan 11/12 - 17/12'
            else note end as note    

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
)

WHERE 1 = 1                                          
AND (note = 'HUB_MODEL_Thuong tai xe guong mau tuan '||'${date_range_week}'
     OR 
     note =  'HUB_MODEL_Thuong tai xe guong mau chu nhat tuan '||'${date_range_sunday}'
     )     
GROUP BY 1) cp 
    on cp.user_id = mw.shipper_id



WHERE 1 = 1
GROUP BY 1,2,ms.sunday_bonus_adjusted,ms.sunday_bonus_original,ms.sunday_bonus_ext,ms.sunday_bonus_x_kpi_adjusted,cp.weekly_paid,cp.sunday_paid
)
WHERE REGEXP_LIKE(LOWER(city_name),'dien bien|test') = false
-- and (sunday_bonus_adjusted != sunday_bonus_original or 
--      weekly_bonus_adjusted != weekly_bonus_orginal )

group by grouping sets(city_name,())

