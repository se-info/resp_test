WITH params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '3' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '3' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '4' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '4' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '5' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '5' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '6' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '6' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '7' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '7' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    ) 



,a as 
     
       (select 1 as mapping
               ,cast(grass_date as date) as report_date 
         from shopeefood.foody_mart__fact_gross_order_join_detail 
         where cast(grass_date as date) >= date'2021-12-01'
         group by 1,2
        )

, b as 
        (SELECT 
        uid as shipper_id
       ,1 as mapping  
       ,a.report_date 
       ,a.city_name
       ,count(distinct case when policy = 2 then ref_order_code else null end) as inshift_order
       ,count(distinct ref_order_code) as total_del
       
from


(SELECT   dot.uid
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE NULL end as city_group
        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        when dot.order_status in (402,403,404) and cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        --,(slot.end_time - slot.start_time)/3600 as shift_hour
        ,sm.city_name
        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy
        ,dot.ref_order_code

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

--LEFT JOIN shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = dot.uid and date(from_unixtime(slot.date_ts - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = (case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        when dot.order_status in (402,403,404) and cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end)
LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86
WHERE 1=1
AND dot.order_status = 400
and dot.pick_city_id not in (238,469)
--and date(from_unixtime(dot.real_drop_time -3600)) between current_date -interval '7' day and current_date - interval '1' day
)a 
where report_date >= date'2021-12-01'

group by 1,2,3,4
        )
,final as 
(SELECT  base1.report_date
       ,base1.city_name
       ,base1.tier
       ,count(distinct case when coalesce(base1.a30_del,0) > 0 then base1.shipper_id else null end) a30_shipper
       ,count(distinct case when coalesce(base1.a7_del,0) > 0 then base1.shipper_id else null end) a7_shipper
       ,count(distinct case when coalesce(base1.a1_del,0) > 0 then base1.shipper_id else null end) a1_shipper


FROM
        (
        SELECT base.report_date
             ,base.shipper_id
             ,base.city_name
             ,case when base.city_name in ('HCM City','Ha Noi City') then coalesce(bonus.current_driver_tier,'No Tier') else 'No Tier' end as tier
             ,case when si.begin_work_date > 0 then date_diff('second',date_trunc('day',from_unixtime(si.begin_work_date)), date_trunc('day',cast(date(base.report_date) as TIMESTAMP)))/(3600*24) else -1 end as seniority
             --- A7 or A30 
             ,sum(case when base.date_ between date(base.report_date) - interval '29' day and date(base.report_date) then base.total_del else 0 end) a30_del
             ,sum(case when base.date_ between date(base.report_date) - interval '6' day and date(base.report_date) then base.total_del else 0 end) a7_del
             ,sum(case when base.date_ between date(base.report_date) - interval '1' day and date(base.report_date) then base.total_del else 0 end) a1_del
        
        FROM
                (
                Select s1.report_date
                      ,s2.report_date as date_
                      ,s2.shipper_id
                      ,s2.city_name
                      ,coalesce(s2.total_del,0) total_del
                
                FROM (select * from a) s1 
                
                LEFT JOIN (select * from b) s2 on s1.mapping = s2.mapping and s2.report_date <= s1.report_date
                
                WHERE 1 = 1 
                and city_name is not null
                )base 
                
        LEFT JOIN 
                (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
                        ,bonus.uid as shipper_id
                        ,driver_type.shipper_type_id
    
                        
                        ,case when driver_type.shipper_type_id = 12 then 'Hub' 
                            when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
                            when bonus.tier in (2,7,12) then 'T2'
                            when bonus.tier in (3,8,13) then 'T3'
                            when bonus.tier in (4,9,14) then 'T4'
                            when bonus.tier in (5,10,15) then 'T5'
                            else null end as current_driver_tier

                
                FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
                left join 
                            (SELECT base.shipper_id
                                ,base.report_date
                                ,base.shipper_type_id
                            
                                From
                                (SELECT shipper_id
                                    ,city_name
                                    ,case when grass_date = 'current' then date(current_date)
                                        else cast(grass_date as date) end as report_date
                                    ,shipper_type_id    
                                
                                    from shopeefood.foody_mart__profile_shipper_master
                                    
                                    where 1=1
                                   -- and (grass_date = 'current' OR cast(grass_date as date) >= date(current_date) - interval '60' day)
                                    and shipper_type_id <> 3
                                    and shipper_status_code = 1
                                    and shipper_type_id = 12 -- hub driver
                                )base 
                                GROUP BY 1,2,3
                            
                            )driver_type on driver_type.shipper_id = bonus.uid and driver_type.report_date =  cast(from_unixtime(bonus.report_date - 60*60) as date)
                
                )bonus on base.report_date = bonus.report_date and base.shipper_id = bonus.shipper_id   
        
        LEFT JOIN shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live si on si.uid = base.shipper_id
        
        GROUP BY 1,2,3,4,5
        )base1
where city_name is not null
GROUP BY 1,2,3)



select p.period 
      ,p.days 
      ,city_name
      ,tier 
      ,sum(a30_shipper)/p.days as a30
      ,sum(a7_shipper)/p.days as a7
      ,sum(a1_shipper)/p.days as a1


from final a

inner join params p on a.report_date between p.start_date and p.end_date


group by 1,2,3,4