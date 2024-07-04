select
    *
from
    (select distinct
                oct.id as order_id
                ,oct.status
                ,ward.name
                ,date(from_unixtime(oct.submit_time - 60*60)) as created_date
                ,Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) as created_hour
                ,date_format(from_unixtime(oct.estimated_delivered_time -3600),'%H:%i:%S') as eta
                ,date_format(from_unixtime(oct.final_delivered_time - 3600),'%H:%i:%S') as delivery_time
                ,mpm.merchant_name as merchant_name
                ,case when mpm.city_id = 217 then 'HCM'
                WHEN mpm.city_id = 218 then'HN'
                WHEN mpm.city_id = 219 then 'DN'
                WHEN mpm.city_id = 220 then 'HP'
                ELSE 'OTH' end as city_group
                ,mpm.merchant_id as merchant_id
                ,mpm.merchant_latitude
                ,mpm.merchant_longtitude
                ,mpm.district_name as merchant_district
                ,mpm.address_text
                ,gross.user_address_text
                ,gross.district_name as user_district
                ,oct.is_asap
                ,dot.drop_latitude
                ,dot.drop_longitude
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
LEFT join shopeefood.foody_mart__fact_gross_order_join_detail gross on gross.id = oct.id
left join (select * from shopeefood.foody_mart__profile_merchant_master where grass_date = 'current')mpm on mpm.merchant_id = oct.restaurant_id
left join (select order_id
                                ,create_time as "confirm_time"
                                from (
                                select
                                order_id
                                ,create_time
                                ,row_number() over (partition by order_id order by create_time asc) as "rank"
                                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                where status = 13 group by 1,2
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
                                        where status = 6 )
                                where rank = 1
                                ) pick on pick.order_id = oct.id
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = oct.id
left join shopeefood.foody_delivery_db__ward_tab__reg_daily_s0_live ward on ward.id = dot.pick_ward_id
WHERE 1=1
AND date(from_unixtime(oct.submit_time - 60*60)) between current_date - interval '35' day and current_date - interval '1' day
)
where
    city_group = ('HCM')
    and created_date >= cast(('2021-11-04') as TIMESTAMP)
    and created_date <= cast(('2021-11-05') as TIMESTAMP)