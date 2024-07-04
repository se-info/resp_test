SELECT base.merchant_id as mex_id
,base.merchant_name 
,base.city_group
,base.district_name
,created_date
-- ,base.merchant_latitude
-- ,base.merchant_longtitude
,(count(base.order_id)*1.00/count(distinct base.created_date)) as total_order
,(sum(case when base.status = 7 and base.is_asap = 1  then base.prep_time else null end)/count(case when base.status = 7 and base.is_asap = 1 then base.order_id ELSE NULL end )) as avg_prep_time

FROM
(select distinct
                oct.id as order_id
                ,oct.status
                ,date(from_unixtime(oct.submit_time - 60*60)) as created_date
                ,Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) as created_hour
                ,date_format(from_unixtime(oct.estimated_delivered_time -3600),'%H:%i:%S') as eta 
                ,date_format(from_unixtime(oct.final_delivered_time - 3600),'%H:%i:%S') as delivery_time
                ,mpm.merchant_name as merchant_name                   
                ,case when mpm.city_id = 217 then 'HCM'
                WHEN mpm.city_id = 218 then'HN'
                WHEN mpm.city_id = 219 then 'Da Nang'
                WHEN mpm.city_id = 220 then 'HP'
                WHEN mpm.city_id = 230 then 'Binh Duong'
                WHEN mpm.city_id = 222 then 'Dong Nai'
                ELSE 'OTH' end as city_group
                ,mpm.merchant_id as merchant_id
                ,date_format(from_unixtime(cfm.confirm_time -3600),'%H:%i:%S') as confirm_time---cfm.confirm_time
                ,date_format(from_unixtime(pick.pick_time -3600),'%H:%i:%S') as pick_time---pick.pick_time
                ,(pick.pick_time - cfm.confirm_time) / 60 as prep_time
                ,mpm.merchant_latitude
                ,mpm.merchant_longtitude  
                ,mpm.district_name
                ,oct.is_asap
from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct
left join (select * from shopeefood.foody_mart__profile_merchant_master where grass_date = 'current')mpm on mpm.merchant_id = oct.restaurant_id 
left join (select order_id
                                ,create_time as "confirm_time"
                                from (
                                select
                                order_id
                                ,create_time
                                ,row_number() over (partition by order_id order by create_time asc) as "rank"
                                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                where status = 13 --- confirm ----
                                group by 1,2
                                )
                                
                        where rank = 1        
                    ) cfm on cfm.order_id = oct.id
left join ( select order_id
                                ,create_time as "pick_time"
                                from (
                                        select
                                        order_id
                                        ,create_time
                                        ,row_number() over (partition by order_id order by create_time asc) as "rank"
                                        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                        where status = 6 --- pick ----
                                    )
                                where rank = 1    
                                --group by 1
                    ) pick on pick.order_id = oct.id

WHERE 1=1
AND date(from_unixtime(oct.submit_time - 60*60)) between current_date - interval '30' day and current_date - interval '1' day
and date(from_unixtime(oct.submit_time - 60*60)) != date('2021-01-15')
)base
WHERE 1=1
AND base.city_group in ('Binh Duong','Dong Nai')
GROUP BY 1,2,3,4,5,6

