with raw as 
(SELECT
            shipper_id
            , create_date AS grass_date
            , CAST(DATE_DIFF('second', actual_start_time_online, actual_end_time_online) AS DOUBLE) / 3600 AS total_online_time
            , CAST(DATE_DIFF('second', actual_start_time_work, actual_end_time_work) AS DOUBLE) / 3600 AS total_working_time
        FROM
            (SELECT
                uid AS shipper_id
                ,DATE(from_unixtime(create_time - 3600)) AS create_date
                ,FROM_UNIXTIME(check_in_time - 3600) AS actual_start_time_online
                ,GREATEST(from_unixtime(check_out_time - 3600),from_unixtime(order_end_time - 3600)) AS actual_end_time_online
                ,IF(order_start_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_start_time - 3600)) AS actual_start_time_work
                ,IF(order_end_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_end_time - 3600)) AS actual_end_time_work
                FROM shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live
                WHERE 1=1
                AND check_in_time > 0
                AND check_out_time > 0
                AND check_out_time >= check_in_time
                AND order_end_time >= order_start_time
                AND ((order_start_time = 0 AND order_end_time = 0)
                    OR (order_start_time > 0 AND order_end_time > 0 AND order_start_time >= check_in_time AND order_start_time <= check_out_time)
                    )
                    )
)
,driver_time as 
(SELECT r.shipper_id
        ,r.grass_date
        ,sm.city_name
        ,case when sm.shipper_type_id = 12 then 'Hub'
              else 'Non Hub' end as shipper_type
        ,sum(total_online_time) as total_online_time
        ,sum(total_working_time) as total_working_time

from raw r 
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = r.shipper_id and try_cast(sm.grass_date as date) = r.grass_date
where r.total_online_time > 0 
and sm.grass_date != 'current'
group by 1,2,3,4
)

,ado as 

(SELECT  dot.uid 
        ,dot.pick_city_id 
        ,city.name_en as city_name
        ,dot.ref_order_id 
        ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date 
        ,row_number()over(partition by dot.uid,date(from_unixtime(dot.real_drop_time - 3600)) order by from_unixtime(dot.real_drop_time - 3600) desc ) as rank 


from shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot 
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

where 1 =1 
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '60' day and current_date - interval '1' day 
)
,final as
(select r.grass_date
      ,case when r.grass_date between date('2022-01-01') and date('2022-01-02') then 202152
            else YEAR(r.grass_date)*100 + WEEK(r.grass_date) end as created_year_week    
      ,r.shipper_type
      ,r.city_name     
      ,count(r.shipper_id) as total_online_driver



from driver_time r 

left join ado o on o.uid = r.shipper_id and r.grass_date = o.report_date 


where 1 = 1 
and r.grass_date between current_date - interval '30' day and current_date - interval '1' day
and o.rank = 1 

group by 1,2,3,4
)

SELECT a.* 


FROM final a 


UNION ALL 

SELECT b.grass_date
      ,b.created_year_week
      ,'1. All' as shipper_type 
      ,b.city_name 
      ,sum(b.total_online_driver) total_online_driver
      
FROM final b 
group by 1,2,3,4
    


