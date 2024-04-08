WITH assignment as 
(select * 

from dev_vnfdbi_opsndrivers.phong_temp_assign
where status in (3,4)

)
,raw as 
(SELECT 
             dot.ref_order_code
            ,dot.ref_order_category
            ,case when from_unixtime(dot.real_drop_time - 60*60) > from_unixtime(dot.estimated_drop_time - 60*60) then 1 else 0 end as is_late_eta
            ,case when  cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift_order
            ,case when cast(json_extract(dotet.order_data,'$.hub_id') as bigint) > 0 then 1 else 0 end as is_hub_qualified 
            ,case when sa.experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted
            ,case when sa.experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign
            ,CASE 
                    WHEN dot.group_id > 0 AND sa.order_type = 'Group' THEN 1       
                    WHEN dot.group_id > 0 AND sa.order_type != 'Group' THEN 2
                    ELSE 0 END AS is_stack_group_order
            ,dot.group_id
            ,sa.order_type
            ,sa.assign_type
            ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
            ,HOUR(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_hour                           
            ,ogm.total_order_in_group
            ,oct.restaurant_id AS merchant_id
            ,mm.merchant_name
            ,city.name_en AS city_name
            ,sa.create_time as incharge_time 
            ,FROM_UNIXTIME(dot.real_pick_time - 3600) AS pick_time
            ,ar.ar_mex
            ,ar.ar_user
            ,CASE 
                WHEN dot.pick_city_id = 217 THEN 'HCM'
                WHEN dot.pick_city_id = 218 THEN 'HN'
                WHEN dot.pick_city_id = 219 THEN 'DN'
                ELSE 'OTH' END AS city_group 
            ,date_diff('second',sa.create_time,ar.ar_mex)/CAST(60 AS DOUBLE) as incharged_to_arrive_mex
            ,date_diff('second',sa.create_time,FROM_UNIXTIME(dot.real_pick_time - 3600))/CAST(60 AS DOUBLE) as incharged_to_pick
            ,case 
                    when (date_diff('second',sa.create_time,ar.ar_mex)/CAST(60 AS DOUBLE)) > 0 then 1 else 0 
                    end as is_valid_lt_incharge_to_arrived
            ,case 
                    when (date_diff('second',sa.create_time,FROM_UNIXTIME(dot.real_pick_time - 3600))/CAST(60 AS DOUBLE)) > 0 then 1 else 0 
                    end as is_valid_lt_incharge_to_pick
            
            ,case when dot.delivery_distance/CAST(1000 AS DOUBLE) <= 1 then 30
                    when dot.delivery_distance/CAST(1000 AS DOUBLE) > 1 then least(60,30 + 5*(ceiling((dot.delivery_distance/CAST(1000 AS DOUBLE))) -1))
                    else null end as lt_sla
            ,DATE_DIFF('second',FROM_UNIXTIME(dot.submitted_time - 3600),FROM_UNIXTIME(dot.real_drop_time - 3600))/CAST(60 AS DOUBLE) AS lt_completion
            ,CASE WHEN dot.real_pick_time > 0 AND dot.estimated_pick_time > 0 
                       THEN (CASE WHEN dot.real_pick_time > dot.estimated_pick_time THEN 1 ELSE 0 END)
                       ELSE null END AS is_late_pickup
            ,dot.is_asap                                                 

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    on dot.id = dotet.order_id

LEFT JOIN (select 
                 order_id
                 ,MAX( CASE WHEN destination_key = 256 THEN FROM_UNIXTIME(create_time - 3600) ELSE NULL END) AS ar_mex
                 ,MAX( CASE WHEN destination_key = 512 THEN FROM_UNIXTIME(create_time - 3600) ELSE NULL END) AS ar_user

          from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_arrive_log_tab_vn_da 
          where date(dt) = current_date - interval '1' day
          group by 1
          ) ar
    on ar.order_id = dot.id

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
            WHERE DATE(dt) = current_date - interval '1' day 
            AND DATE(FROM_UNIXTIME(submit_time - 3600)) BETWEEN current_date - interval '60' day AND current_date - interval '1' day  
            ) oct 
    on oct.id = dot.ref_order_id
    and dot.ref_order_category = 0

LEFT JOIN assignment sa 
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

LEFT JOIN
    (SELECT 
             group_id
            ,ref_order_category
            ,count(distinct ref_order_code) as total_order_in_group
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
    WHERE DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '90' day and current_date - interval '1' day
    group by 1,2
    )  ogm 
        on ogm.group_id =  dot.group_id
        and ogm.ref_order_category = dot.ref_order_category        

LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm 
    on mm.merchant_id = oct.restaurant_id 
    and try_cast(mm.grass_date as date) =  DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = dot.pick_city_id 
    AND city.country_id = 86

WHERE 1 = 1
AND sa_filter.ref_order_id is null 
AND dot.order_status = 400
AND dot.ref_order_category = 0
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN current_date - interval '60' day AND current_date - interval '1' day
)
SELECT
         report_date
        ,report_hour AS grass_hour
        ,city_group

        ,COUNT(DISTINCT ref_order_code) AS nowfood_ado
        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 1 THEN ref_order_code ELSE NULL END) AS _nowfood_group_ado
        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) AS _nowfood_stack_ado
        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) AS _nowfood_single_ado

        ,COUNT(DISTINCT CASE WHEN is_late_eta = 1 THEN ref_order_code ELSE NULL END) AS _nowfood_late_eta
        ,COUNT(DISTINCT CASE WHEN is_late_eta = 1 AND is_stack_group_order = 1 THEN ref_order_code ELSE NULL END) AS _nowfood_group_late_eta
        ,COUNT(DISTINCT CASE WHEN is_late_eta = 1 AND is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) AS _nowfood_stack_late_eta
        ,COUNT(DISTINCT CASE WHEN is_late_eta = 1 AND is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) AS _nowfood_single_late_eta


        ,SUM(CASE WHEN is_valid_lt_incharge_to_arrived = 1 THEN incharged_to_arrive_mex ELSE 0 END)
            /CAST(COUNT(DISTINCT CASE WHEN is_valid_lt_incharge_to_arrived = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS incharge_to_arrive_mex
        ,SUM(CASE WHEN is_stack_group_order = 1 AND is_valid_lt_incharge_to_arrived = 1 THEN incharged_to_arrive_mex ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 1 AND is_valid_lt_incharge_to_arrived = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS group_incharge_to_arrive_mex

        ,SUM(CASE WHEN is_stack_group_order = 2 AND is_valid_lt_incharge_to_arrived = 1 THEN incharged_to_arrive_mex ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 2 AND is_valid_lt_incharge_to_arrived = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS stack_incharge_to_arrive_mex

        ,SUM(CASE WHEN is_stack_group_order = 0 AND is_valid_lt_incharge_to_arrived = 1 THEN incharged_to_arrive_mex ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 0 AND is_valid_lt_incharge_to_arrived = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS single_incharge_to_arrive_mex
            
        ,SUM(CASE WHEN is_valid_lt_incharge_to_pick = 1 THEN incharged_to_pick ELSE 0 END)
            /CAST(COUNT(DISTINCT CASE WHEN is_valid_lt_incharge_to_pick = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS incharged_to_pick
        ,SUM(CASE WHEN is_stack_group_order = 1 AND is_valid_lt_incharge_to_pick = 1 THEN incharged_to_pick ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 1 AND is_valid_lt_incharge_to_pick = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS group_incharged_to_pick

        ,SUM(CASE WHEN is_stack_group_order = 2 AND is_valid_lt_incharge_to_pick = 1 THEN incharged_to_pick ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 2 AND is_valid_lt_incharge_to_pick = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS stack_incharged_to_pick

        ,SUM(CASE WHEN is_stack_group_order = 0 AND is_valid_lt_incharge_to_pick = 1 THEN incharged_to_pick ELSE 0 END)/CAST(
            COUNT(DISTINCT CASE WHEN is_stack_group_order = 0 AND is_valid_lt_incharge_to_pick = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS single_incharged_to_pick

        ,COUNT(DISTINCT CASE WHEN is_asap = 1 AND lt_completion > lt_sla THEN ref_order_code ELSE NULL END)
               /CAST(COUNT(DISTINCT CASE WHEN is_asap = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE)  AS late_sla

        ,COUNT(DISTINCT CASE WHEN is_asap = 1 AND lt_completion > lt_sla AND is_stack_group_order = 1 THEN ref_order_code ELSE NULL END)
               /CAST(COUNT(DISTINCT CASE WHEN is_asap = 1 AND is_stack_group_order = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE)  AS group_late_sla
               
        ,COUNT(DISTINCT CASE WHEN is_asap = 1 AND lt_completion > lt_sla AND is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) 
               /CAST(COUNT(DISTINCT CASE WHEN is_asap = 1 AND is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS stack_late_sla

        ,COUNT(DISTINCT CASE WHEN is_asap = 1 AND lt_completion > lt_sla AND is_stack_group_order = 0 THEN ref_order_code ELSE NULL END)
               /CAST(COUNT(DISTINCT CASE WHEN is_asap = 1 AND is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) AS DOUBLE)  AS single_late_sla


        ,COUNT(DISTINCT CASE WHEN is_late_pickup = 1 THEN ref_order_code ELSE NULL END) 
               /CAST(COUNT(DISTINCT ref_order_code) AS DOUBLE) AS late_pickup_late

        ,COUNT(DISTINCT CASE WHEN is_late_pickup = 1 AND is_stack_group_order = 1 THEN ref_order_code ELSE NULL END)
              /CAST(COUNT(DISTINCT CASE WHEN is_stack_group_order = 1 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS group_late_pickup_late

        ,COUNT(DISTINCT CASE WHEN is_late_pickup = 1 AND is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) 
               /CAST(COUNT(DISTINCT CASE WHEN is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS stack_late_pickup_late

        ,COUNT(DISTINCT CASE WHEN is_late_pickup = 1 AND is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) 
               /CAST(COUNT(DISTINCT CASE WHEN is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) AS DOUBLE) AS single_late_pickup_late

FROM raw d


GROUP BY 1,2,3