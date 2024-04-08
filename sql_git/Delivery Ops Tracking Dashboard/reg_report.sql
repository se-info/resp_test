WITH raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-11-01',current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,assignment as 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type
       ,dot.order_status
    --    ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy                           

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi
    on ogi.id = (CASE WHEN sa.order_type = 200 THEN sa.order_id ELSE 0 END)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
    and ogm.create_time <= sa.create_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_id ELSE sa.order_id END) 
    and dot.ref_order_category = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_category ELSE sa.order_type END)

WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN date'2022-11-01' AND current_date - interval '1' day
)
,driver_income AS 
(select 

    -- reference_id as order_id,
    user_id,
    date(from_unixtime(create_time - 3600)) AS created_date,  
    date(from_unixtime(create_time - 3600)) - interval '1' day AS hub_date,  
    sum(case when txn_type in (104,201,301,2101,2001,3000,1000,401,906) then (balance + deposit)*1.00/100 else 0 end) as shipping_share, -- 104: delivery, 201: NS_user, 301: NS_merchant, 2101: Multidrop, 2001: Sameday, 3000: SPX
  --  sum(case when txn_type in (105,304) then (balance + deposit)*1.00/100 else 0 end) as additional_bonus, -- 105: delivery, 304: NS_merchant
    sum(case when txn_type in (114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137) then (balance + deposit)*1.00/100 else 0 end) as other_bonus, -- bwf for quit order
    sum(case when txn_type in (106,203,2003,303,1002,1005,1007,3002,3005,3007) then (balance + deposit)*1.00/100 else 0 end) as other_payable,
    sum(case when txn_type in (202,302,2002,2102,1001,402,3001) then (balance + deposit)*1.00/100 else 0 end) as return_fee_share, -- 202: NS_user, 302: NS_merchant
    sum(case when txn_type in (101,200,2000,2100,300,1006,3006,400,105) then (balance + deposit)*1.00/100 else 0 end) as bonus, -- order complete bonus
    sum(case when txn_type in (204,2004,304,2106,1003,3003,404) then (balance + deposit)*1.00/100 else 0 end) as bonus_shipper -- additional_bonus
    ,sum(case when txn_type in (512,560,900,901,907) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus
    ,sum(case when txn_type in (134,135,154,108,110,111) then (balance + deposit)*1.00/100 else 0 end) as tip
    ,count(case when txn_type in (134,135,108,110) and (balance + deposit) > 0 then reference_id else null end) as tip_txn

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
where 1=1
and txn_type in 
(
104,201,301,2101,2001,3000,1000,401,906, -- shipping_share   
114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137, -- other bonus 
106,203,2003,303,1002,1005,1007,3002,3005,3007, -- other_payable
202,302,2002,2102,1001,402,3001, -- return_share 
101,200,2000,2100,300,1006,3006,400, -- order completed bonus 
204,2004,304,2106,1003,3003,404, -- additional bonus 
134,135,108,110, --- tipped 
512,560,900,901,907 --bonus             
) 
and date(from_unixtime(create_time - 3600)) BETWEEN date'2022-11-01' and current_date
GROUP BY 1,2
) 
,raw AS
(SELECT 
       dot.uid AS shipper_id
      ,dot.group_id  
      ,dot.ref_order_code
      ,dot.ref_order_id
      ,CASE
           WHEN dot.ref_order_category = 0 THEN '1. Delivery'
           ELSE '2. SPXI' END AS source
      ,city.name_en AS city_name
      ,dot.delivery_distance/CAST(1000 AS DOUBLE) AS distance
      ,dot.delivery_cost/CAST(100 AS DOUBLE) AS delivery_cost
      ,CASE
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 1 THEN '5 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 2 THEN '8 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 3 THEN '10 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 4 THEN '3 hour shift'
             ELSE 'Non Hub' end as hub_type_v1
      ,sa.create_time AS inflow_timestamp 
      ,DATE(sa.create_time) AS inflow_date
      ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
      ,CASE WHEN cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_hub
      ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
      ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') as double) as return_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
      ,CASE 
           WHEN sa.order_type = 'Group' THEN '7. Group Assign'
           ELSE sa.assign_type END AS assign_type   
    --   ,ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.ref_order_id ASC) AS rank  

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
     on dot.id = doet.order_id


-- location
LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
     on city.id = dot.pick_city_id 
     and city.country_id = 86

LEFT JOIN assignment sa 
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

WHERE 1 = 1 
AND sa_filter.order_id is null
AND dot.order_status in (400,401,405,407)
AND DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) BETWEEN date'2022-11-01' and current_date - interval '1' day 
)
,summary AS 
(SELECT 
         raw.report_date AS inflow_date 
        ,raw.shipper_id
        ,DATE(FROM_UNIXTIME(sp.create_time - 3600)) AS onboarded_date
        ,online_time AS online_hour
        ,work_time AS work_hour
        ,CASE 
             WHEN sm.shipper_type_id = 12 THEN 'Hub'
             ELSE 'Non Hub' END AS working_type
        ,di_base.shipping_share
        ,di_bonus.daily_bonus   
        ,di_tip.tip  
        ,di_tip.tip_txn
        ,COUNT(DISTINCT raw.ref_order_code) AS total_order
        ,COUNT(DISTINCT CASE WHEN raw.source = '1. Delivery' THEN raw.ref_order_code ELSE NULL END) AS total_order_food
        ,COUNT(DISTINCT CASE WHEN raw.is_hub = 1 THEN raw.ref_order_code ELSE NULL END) AS hub_order
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 AND raw.assign_type != '7. Group Assign' THEN raw.ref_order_code ELSE NULL END) AS stack_order

FROM raw 

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp 
    on sp.uid = raw.shipper_id

LEFT JOIN (SELECT
                 report_date
                 ,shipper_id
                 ,SUM(online_time) AS online_time 
                 ,SUM(work_time) AS work_time 
           FROM dev_vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot
           GROUP BY 1,2      
            ) rp 
    on rp.shipper_id = raw.shipper_id 
    and rp.report_date = raw.report_date

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id
    and try_cast(sm.grass_date AS date) = raw.report_date

LEFT JOIN driver_income di_base
    on di_base.user_id = raw.shipper_id 
    and (CASE WHEN sm.shipper_type_id = 12 THEN di_base.hub_date = raw.report_date
              ELSE di_base.created_date = raw.report_date END) 

LEFT JOIN driver_income di_bonus
    on di_bonus.user_id = raw.shipper_id 
    and di_bonus.hub_date = raw.report_date

LEFT JOIN driver_income di_tip
    on di_tip.user_id = raw.shipper_id 
    and di_tip.created_date = raw.report_date


GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT
        p.period
       ,SUM(total_order)/CAST(COUNT(DISTINCT s.inflow_date) AS DOUBLE) AS net_ado 
       ,SUM(hub_order)/CAST(SUM(total_order) AS DOUBLE) AS pct_hub
       ,SUM(stack_order)/CAST(SUM(total_order) AS DOUBLE) AS pct_stack
       ,SUM(total_order)/CAST(COUNT(shipper_id) AS DOUBLE) AS ado_driver
       ,SUM(online_hour)/CAST(COUNT(shipper_id) AS DOUBLE) AS online 
       ,SUM(CASE WHEN working_type = 'Hub' THEN online_hour ELSE NULL END)/CAST(COUNT(CASE WHEN working_type = 'Hub' THEN shipper_id ELSE NULL END) AS DOUBLE) AS hub_online
       ,SUM(CASE WHEN working_type != 'Hub' THEN online_hour ELSE NULL END)/CAST(COUNT(CASE WHEN working_type != 'Hub' THEN shipper_id ELSE NULL END) AS DOUBLE) AS non_hub_online
       ,SUM(work_hour)/CAST(SUM(online_hour) AS DOUBLE) AS utilization_rate
       ,(SUM(shipping_share) + SUM(daily_bonus) + SUM(tip))/CAST(COUNT(shipper_id) AS DOUBLE) AS earning
       ,SUM(shipping_share)/CAST(COUNT(shipper_id) AS DOUBLE) AS organic
       ,SUM(daily_bonus)/CAST(COUNT(shipper_id) AS DOUBLE) AS non_organic
       ,SUM(tip)/CAST(COUNT(shipper_id) AS DOUBLE) AS tip
       ,SUM(tip_txn)/CAST(SUM(total_order) AS DOUBLE) AS pct_tip_ado 
       ,COUNT(shipper_id)/CAST(COUNT(DISTINCT s.inflow_date) AS DOUBLE) AS transacting_driver
       ,COUNT(CASE WHEN working_type = 'Hub' THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT s.inflow_date) AS DOUBLE) AS transacting_driver_hub 
       ,COUNT(CASE WHEN working_type != 'Hub' THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT s.inflow_date) AS DOUBLE) AS transacting_driver_non_hub 
       ,COUNT(CASE WHEN online_hour >= 7.5 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT s.inflow_date) AS DOUBLE) AS transacting_driver_fulltime

FROM summary s 

INNER JOIN params_date p 
     on s.inflow_date BETWEEN p.start_date and p.end_date


GROUP BY 1     