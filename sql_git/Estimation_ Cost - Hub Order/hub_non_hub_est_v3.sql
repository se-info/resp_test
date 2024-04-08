-- Temp table
-- DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_group_temp; 
-- CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.phong_group_temp AS
WITH assignment as 
(SELECT 
        *

FROM dev_vnfdbi_opsndrivers.phong_raw_order_v2
WHERE order_type = 0 
AND order_status = 'Delivered'
)
,group_raw AS
(SELECT 
        dot.ref_order_id
       ,dot.ref_order_code
       ,dot.ref_order_category
       ,dot.group_id
       ,ogi.group_code
       ,ogi.ref_order_category AS group_category
       ,city.name_en AS city_name
       ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
    --    ,dotet.order_data
       ,CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS DOUBLE) AS driver_policy
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a 
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack       
       ,dot.delivery_cost/CAST(100 AS DOUBLE) AS single_fee
       ,dot.delivery_distance/CAST(1000 AS DOUBLE) AS single_distance   
       ,ogi.distance/CAST(100000 AS DOUBLE) AS group_distance
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS final_stack_fee 
       ,ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) AS rank_order
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate_single

       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_1
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_2             
    --    ,ogi.extra_data 
       ,sa.assign_type
       ,sa.order_assign_type
       ,GREATEST(
                12500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                ) AS single_fee_est
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN GREATEST(
                12500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_1_est
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN GREATEST(
                12500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_2_est                 

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '2' day ) dot 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '2' day) dotet
    on dotet.order_id = dot.id

LEFT JOIN assignment sa 
    on sa.id = dot.ref_order_id 
    and sa.order_type = dot.ref_order_category 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '2' day) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city
    on city.id = dot.pick_city_id and city.country_id = 86

WHERE 1 = 1 
AND dot.group_id > 0 
AND dot.ref_order_category = 0
AND dot.order_status = 400

AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-04-01' and current_date - interval '2' day
)
,group_info AS 
(SELECT 
        m.group_code
       ,m.group_id 
       ,m.group_category
       ,m.city_name
       ,m.report_date
       ,ARRAY_AGG(m.ref_order_code) AS ref_order_code_ext
       ,COUNT(DISTINCT m.ref_order_code) AS total_order_in_group
       ,SUM(CASE WHEN m.rank_order = 1 THEN m.extra_fee ELSE NULL END) AS extra_fee
       ,SUM(CASE WHEN m.rank_order = 1 then m.re_stack ELSE NULL END) AS re_system 
       ,SUM(m.fee_1) AS fee_1
       ,SUM(m.fee_2) AS fee_2
       ,SUM(m.fee_1_est) AS fee_1_est
       ,SUM(m.fee_2_est) AS fee_2_est        
       ,SUM(CASE WHEN m.rank_order = 1 THEN m.rate_a ELSE NULL END) AS rate_a
       ,SUM(CASE WHEN m.rank_order = 1 THEN m.rate_b ELSE NULL END) AS rate_b
       ,SUM(CASE WHEN m.rank_order = 1 then ROUND((m.unit_fee * m.group_distance * m.surge_rate),1) ELSE NULL END) AS total_shipping_fee
       ,SUM(CASE WHEN m.rank_order = 1 then min_fee ELSE NULL END) AS min_shipping_fee_current
       ,SUM(CASE WHEN m.rank_order = 1 then final_stack_fee ELSE NULL END) AS final_stack_fee_current
       ,SUM(single_distance) AS sum_single_distance
       ,SUM(single_fee) AS sum_single_fee 
       ,SUM(single_fee_est) AS sum_single_fee_est 

FROM group_raw m
WHERE 1 = 1
GROUP BY 1,2,3,4,5
)

SELECT
         gi.group_id
        ,gi.group_code
        ,gi.report_date
        ,gi.city_name
        ,gi.group_category
        ,gi.min_shipping_fee_current
        ,gi.final_stack_fee_current
        ,extra_fee * 2 * (total_order_in_group - 1) AS extra_fee
        ,(final_stack_fee_current - GREATEST(min_shipping_fee_current,total_shipping_fee)) as extra_fee_cal 
        ,gi.total_order_in_group
        ,GREATEST(
                   (GREATEST(12500,fee_1_est) + (GREATEST(12500,fee_2_est)/re_system) * gi.rate_a),(GREATEST(12500,fee_2_est) + (GREATEST(12500,fee_1_est)/re_system) * gi.rate_b)
                 ) AS min_fee_est    
        ,ROUND(gi.total_shipping_fee,0) as total_shipping_fee              
        ,CASE
            WHEN total_order_in_group = 1 THEN sum_single_fee
            WHEN total_order_in_group = 2 THEN 
            LEAST(GREATEST (
                      GREATEST(
                       (GREATEST(13500,fee_1) + (GREATEST(13500,fee_2)/re_system) * gi.rate_a),(GREATEST(13500,fee_2) + (GREATEST(13500,fee_1)/re_system) * gi.rate_b)
                     ),  
                     ROUND(gi.total_shipping_fee,0)
                      ),sum_single_fee) +  (final_stack_fee_current - GREATEST(min_shipping_fee_current,total_shipping_fee)) 
                      
            WHEN total_order_in_group > 2 THEN
             sum_single_fee * 1 +  (final_stack_fee_current - GREATEST(min_shipping_fee_current,total_shipping_fee))
                    END  AS final_stack_fee_cal

        ,CASE
            WHEN total_order_in_group = 1 THEN sum_single_fee_est 
            WHEN total_order_in_group = 2 THEN 
            LEAST(GREATEST (
                      GREATEST(
                       (fee_1_est + (fee_2_est/re_system) * gi.rate_a),(fee_2_est + (fee_1_est/re_system) * gi.rate_b)
                     ), 
                     ROUND(gi.total_shipping_fee,0)
                      ),sum_single_fee_est) +  (final_stack_fee_current - GREATEST(min_shipping_fee_current,total_shipping_fee)) 
                      
            WHEN total_order_in_group > 2 THEN
             sum_single_fee_est * 1 +  (final_stack_fee_current - GREATEST(min_shipping_fee_current,total_shipping_fee))
                    END  AS final_stack_fee_est                    
        ,sum_single_distance
        ,sum_single_fee
        ,sum_single_fee_est
FROM group_info gi


WHERE group_id = 41659639
;
-- Simulate

WITH raw_order AS
(SELECT 
        raw.*
    --    ,CASE WHEN cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_hub
       ,CASE WHEN delivered_by = 'hub' THEN 1 ELSE 0 END AS is_hub
       ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
       ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') as double) as return_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
       ,case when is_stack_group_order in (1,2) then 'Stack' else 'non-stack' end as order_type        


                                                                                     
FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level raw

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
                    where date(dt) = current_date - interval '2' day
                    and date(from_unixtime(submitted_time - 3600)) BETWEEN current_date - interval '90' day and current_date - interval '1' day
                    ) dot
     on dot.ref_order_id = raw.order_id 
     and dot.ref_order_category = 0

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
     on dot.id = doet.order_id
WHERE 1 = 1 
and raw.source in ('Food')
AND grass_date BETWEEN date'2023-04-01' AND date'2023-04-27'
-- AND grass_date = date'2023-05-06'
)
,group_info AS 
(SELECT
        *
FROM dev_vnfdbi_opsndrivers.phong_group_temp
-- WHERE min_fee_est is null 
-- AND total_order_in_group < 3 
)
,hub_metrics as 
(
SELECT 
        date_,uid,sum(total_order) AS total_order,sum(extra_ship) AS extra_ship ,sum(new_extra) AS new_extra
FROM     
(SELECT 
         date_
        ,uid
        ,hub_type_original
        ,total_order
        ,extra_ship
        ,12500*(extra_ship/13500) AS new_extra  
        

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
)
GROUP BY 1,2
)
,summary AS
(SELECT 
       ro.order_id as id
      ,ro.partner_id 
      ,CAST(ro.order_id AS VARCHAR)||CAST(ro.partner_id AS VARCHAR) AS order_code
      ,COALESCE(ro.group_id,0) AS group_id
      ,ro.is_hub
    --   ,COALESCE(gi.original_fee * (ro.distance/CAST(gi.single_distance_sum AS DOUBLE)),0) AS group_fee_allocate 
      ,ro.dotet_total_shipping_fee
      ,round(GREATEST(ro.min_fee,(ro.unit_fee * ro.distance * ro.surge_rate),0),0) AS fee_recal
      ,order_type
      ,ROUND(CASE 
            WHEN ro.is_hub = 0 AND COALESCE(ro.group_id,0) = 0 THEN ro.dotet_total_shipping_fee
            WHEN ro.is_hub = 0 AND COALESCE(ro.group_id,0) > 0 THEN COALESCE(gi.final_stack_fee_current * (ro.distance/CAST(gi.sum_single_distance AS DOUBLE)),gi.final_stack_fee_current)
            ELSE 13500 + ro.driver_cost_surge END,0) AS current_shipping_fee
       ,ROUND(CASE 
            WHEN ro.is_hub = 0 AND COALESCE(ro.group_id,0) = 0 THEN GREATEST(12500,ro.unit_fee*ro.distance*ro.surge_rate)
            WHEN ro.is_hub = 0 AND COALESCE(ro.group_id,0) > 0 THEN COALESCE(gi.final_stack_fee_est * (ro.distance/CAST(gi.sum_single_distance AS DOUBLE)),gi.final_stack_fee_current)
            WHEN ro.is_hub = 1 AND ro.distance <= 3 THEN 12500 + ro.driver_cost_surge
            WHEN ro.is_hub = 1 AND ro.distance <= 4 THEN 13500 + ro.driver_cost_surge
            WHEN ro.is_hub = 1 AND ro.distance > 4 THEN 14500 + ro.driver_cost_surge
            END,0) AS new_shipping_fee  
       ,ro.grass_date AS report_date
       ,ro.city_name
       ,ro.driver_cost_base + ro.driver_cost_surge as base_v1 
       ,ro.driver_cost_base_v2 + ro.driver_cost_surge_v2 as base_v2               
       ,ROUND(ro.driver_cost_base + ro.driver_cost_surge + return_fee_share_basic + return_fee_share_surge,0) AS dr_cost_base_n_surge_v1
       ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end) as dr_cost_bonus_v1        
       ,ROUND(ro.driver_cost_base_v2 + ro.driver_cost_surge_v2 + return_fee_share_basic + return_fee_share_surge,0) AS dr_cost_base_n_surge_v2
       ,(case 
        when is_nan(bonus) = true then 0.00
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end) as dr_cost_bonus_v2
       
FROM raw_order ro 

LEFT JOIN group_info gi 
     on gi.group_id = ro.group_id
     and gi.group_category = 0
    

WHERE 1 = 1 
)
,final AS                                                       
(SELECT 
-- * from summary where current_shipping_fee < dr_cost_base_n_surge_v2
        --    'M- 202304' period
            s.report_date       
          ,'All' city_name
          ,s.partner_id
          ,s.is_hub
          ,s.order_type
          ,COUNT(DISTINCT s.order_code) AS total_bill
          ,SUM(s.current_shipping_fee) AS cpo_cal  
          ,SUM(dr_cost_base_n_surge_v1 ) AS cpo_current
          ,SUM(s.new_shipping_fee) AS cpo_new
          ,SUM(s.dr_cost_bonus_v1) AS bonus_current
          ,CASE WHEN s.is_hub = 1 AND ds.extra_ship > 0 THEN (SUM(ds.extra_ship)/SUM(ds.total_order))*COUNT(DISTINCT s.order_code) ELSE 0 END AS extra_ship_allocate_current
          ,CASE WHEN s.is_hub = 1 AND ds.extra_ship > 0 THEN (SUM(ds.new_extra)/SUM(ds.total_order))*COUNT(DISTINCT s.order_code) ELSE 0 END AS extra_ship_allocate_new
                        


FROM summary s

LEFT JOIN hub_metrics ds 
    on ds.date_ = s.report_date
    and ds.uid = s.partner_id




GROUP BY 1,2,3,4,5,ds.extra_ship)

-- SELECT * FROM final where partner_id = 2996387
SELECT
        'M- 202304' report_date
       ,'All' AS city_name
       ,CASE 
             WHEN is_hub = 1 then 'hub' ELSE 'non-hub' end as is_hub
       ,order_type
       ,SUM(total_bill) AS total_bill
       ,SUM(cpo_cal+extra_ship_allocate_current)/ex.exchange_rate AS cpo_cal
       ,SUM(cpo_new+extra_ship_allocate_new)/ex.exchange_rate AS cpo_new
       ,SUM(bonus_current)/ex.exchange_rate AS bonus_current
       ,SUM(cpo_current)/ex.exchange_rate AS cpo_current       


FROM final s 

LEFT JOIN mp_order.dim_exchange_rate__reg_s0_live ex 
     on ex.grass_date = s.report_date
     and currency = 'VND'

     
GROUP BY 1,2,3,4,ex.exchange_rate
;
-- Actual

with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-12-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as varchar)
        ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
        ,CAST(date_diff('day',date_trunc('week',report_date),((date_trunc('week',report_date) + interval '7' day))) as double)

from raw_date
WHERE report_date between date'2023-05-08' and date'2023-05-14'

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,CAST(date_trunc('month',report_date) as varchar)
        ,CAST((date_trunc('month',report_date) + interval '1' month - interval '1' day) as varchar) 
        ,CAST(date_diff('day',date_trunc('month',report_date),(date_trunc('month',report_date) + interval '1' month)) as double)

from raw_date
)  
,bill_fee_base as
(select
    *

    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
where 1=1

and date_ >= date'2022-12-01'
and date_ <= date'2023-04-27'
and source in ('Food')
)
,order_stacking_and_hub_raw as
(select 
    -- date_trunc('month',grass_date) as report_month
    p.period
    ,case when source in ('Food','Market') then 'Food' else 'Ship' end as source
    ,delivered_by
    ,case when is_stack_group_order in (1,2) then 'stack' else 'non-stack' end as order_type
    ,cast(count(distinct order_id) as double) as total_orders
    -- ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) as double)*(-1) as total_cost
    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) as double) as total_cost
    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd) as double) as base_n_surge
    ,cast(sum(dr_cost_bonus_usd) as double) as bonus
    -- ,sum(total_shipping_fee/exchange_rate) total_cost_before_stack

    -- ,sum(case 
    --     when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate        
    --     else total_shipping_fee/exchange_rate end) as total_cost_after_stack
    

    -- ,sum(case 
    --     when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate
    --     else total_shipping_fee/exchange_rate end)
    -- - 
    -- sum(total_shipping_fee/exchange_rate) as total_cost_saving

from bill_fee_base b 
inner join params_date p 
    on b.grass_date between cast(p.start_date as date) and cast(p.end_date as date)
group by 1,2,3,4
)
select *


from order_stacking_and_hub_raw

;

