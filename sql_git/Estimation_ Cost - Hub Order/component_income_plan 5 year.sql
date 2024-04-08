-- WITH driver_order AS 
-- (SELECT 
--        DATE(FROM_UNIXTIME(dot.real_drop_time -3600)) AS report_date
--        ,uid 
--        ,COUNT(DISTINCT CASE WHEN doet.driver_policy = 2 THEN ref_order_code ELSE NULL END) AS total_order     

-- FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
-- LEFT JOIN (SELECT
--                   order_id
--                   ,CAST(JSON_EXTRACT(order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy
--             FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

-- WHERE 1 = 1 
-- GROUP BY 1,2
-- )
WITH hub_income_tab AS 
(SELECT 
       DATE(FROM_UNIXTIME(report_date - 3600)) AS report_date
      ,uid 
      ,SUM(CAST(json_extract(hub.extra_data,'$.calculated_shipping_shared') AS BIGINT)) AS ship_shared
      ,SUM(COALESCE(CAST(json_extract(hub.extra_data,'$.total_bonus') AS BIGINT),0)) AS daily_bonus
      ,SUM(CASE 
            WHEN CAST(json_extract(hub.extra_data,'$.is_apply_fixed_amount') AS VARCHAR) = 'true' 
                  THEN (CAST(json_extract(hub.extra_data,'$.total_income') AS BIGINT) - CAST(json_extract(hub.extra_data,'$.calculated_shipping_shared') AS BIGINT))
            ELSE 0 END) AS extra_ship
      ,SUM(CAST(json_extract(hub.extra_data,'$.total_income') AS BIGINT)) AS total_income

FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub

GROUP BY 1,2
)
,bonus_tab AS 
(SELECT 
       user_id
      -- ,note
      -- ,txn_type
      -- ,CARDINALITY(split(note,' ')) AS varchar_lenght
      ,TRY(CASE 
            WHEN txn_type IN (505,520) THEN 
                 CAST(date_parse((CAST(split(note,' ')[CARDINALITY(split(note,' '))] AS varchar)||'/'||'23'),'%d/%m/%y') AS DATE)
            WHEN txn_type in (900,906,907) THEN      
                 CAST(split(note,' ')[4] AS DATE) 
                 END) AS date_
      -- ,CAST(date_parse((CAST(cast(split(note,' ') as array<json>)[9] as varchar)||'/'||'23'),'%d/%m/%y') AS DATE) AS hub_pay_date
      -- ,CASE WHEN txn_type IN (906) THEN balance/cast(100 as double) END AS ship_hub
      -- ,CASE WHEN txn_type IN (907) THEN balance/cast(100 as double) END AS bonus_hub
      ,SUM(CASE WHEN txn_type IN (505,520) THEN balance/cast(100 as double) END)  AS weekly_bonus_hub
      -- ,SUM(CASE WHEN txn_type IN (900) THEN balance/cast(100 as double) END) AS daily_bonus_part

from (select * ,case when user_id in (20754253,6872389,21722008,18521518,18448800,21634564,7999552,22285257) and create_time = 1655866978 then 0 else 1 end as is_valid
      from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live
      where 1 = 1 
      AND DATE(FROM_UNIXTIME(create_time - 3600)) >= DATE'2023-07-01'
      and txn_type in (505,520,900,906,907)
AND regexp_like(note,'HUB_MODEL_Thuong tai xe guong mau tuan|HUB_MODEL_Thuong tai xe guong mau chu nhat tuan') = true
)
where 1 = 1  
and is_valid = 1 
GROUP BY 1,2
)
,income_tab AS 
(select 

    -- reference_id as order_id,
    user_id,
    CASE WHEN hm.uid IS NOT NULL THEN 'Hub' ELSE 'Non Hub' END AS driver_type,
    date(from_unixtime(create_time - 3600)) AS created_date,  
    sum(case when txn_type in (104,201,301,2101,2001,3000,1000,401) then (balance + deposit)*1.00/100 else 0 end) as shipping_share_non_hub, -- 104: delivery, 201: NS_user, 301: NS_merchant, 2101: Multidrop, 2001: Sameday, 3000: SPX
  --  sum(case when txn_type in (105,304) then (balance + deposit)*1.00/100 else 0 end) as additional_bonus, -- 105: delivery, 304: NS_merchant
    sum(case when txn_type in (114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137) then (balance + deposit)*1.00/100 else 0 end) as other_bonus, -- bwf for quit order
    sum(case when txn_type in (106,203,2003,303,1002,1005,1007,3002,3005,3007) then (balance + deposit)*1.00/100 else 0 end) as other_payable,
    sum(case when txn_type in (202,302,2002,2102,1001,402,3001) then (balance + deposit)*1.00/100 else 0 end) as return_fee_share, -- 202: NS_user, 302: NS_merchant
    sum(case when txn_type in (101,200,2000,2100,300,1006,3006,400,105) then (balance + deposit)*1.00/100 else 0 end) as bonus, -- order complete bonus
    sum(case when txn_type in (204,2004,304,2106,1003,3003,404) then (balance + deposit)*1.00/100 else 0 end) as bonus_shipper -- additional_bonus
    ,sum(case when txn_type in (134,135,154,108,110,111) then (balance + deposit)*1.00/100 else 0 end) as tip
    ,count(case when txn_type in (134,135,108,110) and (balance + deposit) > 0 then reference_id else null end) as tip_txn

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live txn

left join hub_income_tab hm 
    on hm.uid = txn.user_id
    and hm.report_date = date(from_unixtime(create_time - 3600))

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
GROUP BY 1,2,3
)
,summary AS 
(SELECT 
        base.created_date
       ,base.user_id AS shipper_id 
       ,base.driver_type
       ,COALESCE(SUM(base.shipping_share_non_hub + COALESCE(hi.ship_shared,0) + base.return_fee_share),0) AS ship_shared 
       ,COALESCE(SUM(COALESCE(bn.bonus_value/CAST(100 AS DOUBLE),0) + COALESCE(hi.daily_bonus,0)),0) AS daily_bonus 
       ,COALESCE(SUM(COALESCE(hi.extra_ship,0)),0) AS compensation_fee
      --  ,COALESCE(SUM(COALESCE(bt.weekly_bonus_hub,0)),0) AS attendance_bonus_hub
       ,COALESCE(SUM(base.other_bonus + base.bonus + base.bonus_shipper),0) AS other_bonus
       ,COALESCE(SUM(base.other_payable),0) AS other_fee
       ,COALESCE(SUM(base.tip),0) AS tips
       ,COALESCE(SUM(base.shipping_share_non_hub + COALESCE(hi.ship_shared,0) + base.return_fee_share + COALESCE(bn.bonus_value/CAST(100 AS DOUBLE),0) + COALESCE(hi.daily_bonus,0) + COALESCE(hi.extra_ship,0) 
            + COALESCE(bt.weekly_bonus_hub,0) + base.other_bonus + base.bonus + base.bonus_shipper + base.other_payable + base.tip),0) AS total_income



FROM income_tab base 

left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bn 
    on bn.uid = base.user_id
    and date(from_unixtime(bn.report_date - 3600)) = base.created_date

LEFT JOIN bonus_tab bt
    on bt.user_id = base.user_id 
    and bt.date_ = base.created_date  

LEFT JOIN hub_income_tab hi 
    on hi.uid = base.user_id
    and hi.report_date = base.created_date

WHERE 1 = 1 

AND base.created_date BETWEEN DATE'2023-07-24' AND DATE'2023-07-30'
GROUP BY 1,2,3)
SELECT 
       s.data_period
      ,s.driver_type
      ,TRY(SUM(ship_shared)/COUNT(DISTINCT shipper_id)) AS ship_shared
      ,TRY(SUM(daily_bonus)/COUNT(DISTINCT shipper_id)) AS daily_bonus
      ,TRY(SUM(compensation_fee)/COUNT(DISTINCT shipper_id)) AS compensation_fee
      ,TRY(SUM(other_bonus)/COUNT(DISTINCT shipper_id)) AS other_bonus
      ,TRY(SUM(other_fee)/COUNT(DISTINCT shipper_id)) AS other_fee
      ,TRY(SUM(tips)/COUNT(DISTINCT shipper_id)) AS tips
      ,TRY(SUM(IF(driver_type IN ('long-shift','short-shift'),COALESCE(weekly_bonus_hub,0),0))/COUNT(DISTINCT s.shipper_id)) AS attendance_bonus_hub
      ,TRY(SUM(ship_shared + daily_bonus + compensation_fee + IF(driver_type IN ('long-shift','short-shift'),COALESCE(weekly_bonus_hub,0),0)
            + other_bonus + other_fee + tips)/COUNT(DISTINCT shipper_id)) AS total_income
      ,COUNT(DISTINCT shipper_id) AS a7            
FROM
(SELECT
        'W30: 24 Jul - 30 Jul' AS data_period
       ,s.shipper_id  
       ,CASE WHEN s.driver_type = 'Non Hub' THEN s.driver_type
             WHEN s.driver_type = 'Hub' AND hm.hub_type_original in ('10 hour shift','8 hour shift') THEN 'long-shift'      
             WHEN s.driver_type = 'Hub' AND hm.hub_type_original in ('5 hour shift','3 hour shift') THEN 'short-shift'
             ELSE s.driver_type END AS driver_type      
       ,TRY(SUM(ship_shared)/COUNT(DISTINCT shipper_id)) AS ship_shared
       ,TRY(SUM(daily_bonus)/COUNT(DISTINCT shipper_id)) AS daily_bonus
       ,TRY(SUM(compensation_fee)/COUNT(DISTINCT shipper_id)) AS compensation_fee
       ,TRY(SUM(other_bonus)/COUNT(DISTINCT shipper_id)) AS other_bonus
       ,TRY(SUM(other_fee)/COUNT(DISTINCT shipper_id)) AS other_fee
       ,TRY(SUM(tips)/COUNT(DISTINCT shipper_id)) AS tips
       ,TRY(SUM(ship_shared + daily_bonus + compensation_fee 
                + other_bonus + other_fee + tips)/COUNT(DISTINCT shipper_id)) AS total_income

FROM summary s 

LEFT JOIN 
(SELECT 
         uid 
        ,hub_type_original
        ,COUNT(DISTINCT slot_id) AS total_slot
        ,ROW_NUMBER()OVER(PARTITION BY uid ORDER BY COUNT(DISTINCT slot_id) DESC ) AS rank
        
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
AND date_ between date'2023-07-24' and date'2023-07-30'   
GROUP BY 1,2
) hm on hm.uid = s.shipper_id and hm.rank = 1 


GROUP BY 1,2,3
) s 

LEFT JOIN (SELECT user_id,SUM(weekly_bonus_hub) AS weekly_bonus_hub FROM bonus_tab WHERE weekly_bonus_hub > 0 AND date_ = date'2023-07-30' GROUP BY 1) w 
    on w.user_id = s.shipper_id 



GROUP BY 1,2






