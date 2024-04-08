-- overall
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
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(1 as double)

from raw_date
group by 1,2,3,4,5

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as varchar)
        ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
        ,CAST(date_diff('day',date_trunc('week',report_date),((date_trunc('week',report_date) + interval '7' day))) as double)

from raw_date

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,CAST(date_trunc('month',report_date) as varchar)
        ,CAST((date_trunc('month',report_date) + interval '1' month - interval '1' day) as varchar) 
        ,CAST(date_diff('day',date_trunc('month',report_date),(date_trunc('month',report_date) + interval '1' month)) as double)

from raw_date
) 
,tier_tab as 
(SELECT 
         cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
        ,bonus.uid as shipper_id
        ,CASE WHEN sm.shipper_type_id = 12 THEN 'Hub' ELSE ti.tier_name_en END AS driver_tier
        ,bonus.total_point
        ,bonus.daily_point
        ,completed_rate/CAST(100 AS DOUBLE) AS sla_rate

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = bonus.uid 
    and try_cast(sm.grass_date as date) = date(from_unixtime(bonus.report_date - 3600))

LEFT JOIN shopeefood.foody_internal_db__shipper_tier_config_tab__reg_daily_s0_live ti 
    on ti.tier_id = bonus.tier 
    and ti.city_id = sm.city_id
)
,driver_income AS 
(select 

    -- reference_id as order_id,
    user_id,
    date(from_unixtime(create_time - 3600)) AS created_date,  
    date(from_unixtime(create_time - 3600)) - interval '1' day AS hub_date,  
    sum(case when txn_type in (906) then (balance + deposit)*1.00/100 else 0 end) as shipping_share_hub,
    sum(case when txn_type in (907) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus_hub,
    sum(case when txn_type in (104,201,301,2101,2001,3000,1000,401) then (balance + deposit)*1.00/100 else 0 end) as shipping_share_non_hub, -- 104: delivery, 201: NS_user, 301: NS_merchant, 2101: Multidrop, 2001: Sameday, 3000: SPX
  --  sum(case when txn_type in (105,304) then (balance + deposit)*1.00/100 else 0 end) as additional_bonus, -- 105: delivery, 304: NS_merchant
    sum(case when txn_type in (114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137) then (balance + deposit)*1.00/100 else 0 end) as other_bonus, -- bwf for quit order
    sum(case when txn_type in (106,203,2003,303,1002,1005,1007,3002,3005,3007) then (balance + deposit)*1.00/100 else 0 end) as other_payable,
    sum(case when txn_type in (202,302,2002,2102,1001,402,3001) then (balance + deposit)*1.00/100 else 0 end) as return_fee_share, -- 202: NS_user, 302: NS_merchant
    sum(case when txn_type in (101,200,2000,2100,300,1006,3006,400,105) then (balance + deposit)*1.00/100 else 0 end) as bonus, -- order complete bonus
    sum(case when txn_type in (204,2004,304,2106,1003,3003,404) then (balance + deposit)*1.00/100 else 0 end) as bonus_shipper -- additional_bonus
    ,sum(case when txn_type in (512,560,900,901) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus_non_hub
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
and date(from_unixtime(create_time - 3600)) BETWEEN current_date - interval '120' day and current_date 
GROUP BY 1,2
)
,summary AS
(SELECT
          DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
         ,dot.uid AS shipper_id
         ,sm.shipper_name 
         ,CASE WHEN sm.city_name IN ('HCM City','Ha Noi City','Da Nang City') THEN sm.city_name ELSE 'Other' END AS city_name
         ,COALESCE(tt.driver_tier,'Other') AS driver_tier
         ,rp.total_online_seconds/CAST(3600 AS DOUBLE) AS online_hour
         ,rp.total_work_seconds/CAST(3600 AS DOUBLE) AS work_hour
         ,COALESCE(di_base_hub.shipping_share_hub + di_base_non_hub.shipping_share_non_hub
                + di_base_non_hub.return_fee_share 
                + di_bonus.daily_bonus_hub + di_bonus.daily_bonus_non_hub 
                + di_tip.bonus + di_tip.bonus_shipper + di_tip.other_payable,0) AS driver_income
       ,COALESCE(di_bonus.daily_bonus_hub + di_bonus.daily_bonus_non_hub,0) AS driver_daily_bonus      
         ,COUNT(DISTINCT ref_order_code) AS cnt_order
         

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot


-- LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
--     on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp
    on rp.uid = dot.uid
    and date(from_unixtime(rp.report_date - 3600)) = DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))

LEFT JOIN tier_tab tt 
    on tt.shipper_id = dot.uid
    and tt.report_date = DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = dot.uid
    and try_cast(sm.grass_date as date) = DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))

-- income

LEFT JOIN driver_income di_base_hub
    on di_base_hub.user_id = sm.shipper_id 
    and di_base_hub.hub_date = try_cast(sm.grass_date as date)
              
LEFT JOIN driver_income di_base_non_hub
    on di_base_non_hub.user_id = sm.shipper_id 
    and di_base_non_hub.created_date = try_cast(sm.grass_date as date)

LEFT JOIN driver_income di_bonus
    on di_bonus.user_id = sm.shipper_id 
    and di_bonus.hub_date = try_cast(sm.grass_date as date)

LEFT JOIN driver_income di_tip
    on di_tip.user_id = sm.shipper_id 
    and di_tip.created_date = try_cast(sm.grass_date as date)

WHERE dot.order_status = 400
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2022-12-01' and current_date

GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT 
         p.period_group
        ,p.period
        ,s.city_name
        ,s.driver_tier
        ,SUM(s.cnt_order)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_ado
        ,SUM(s.online_hour)/CAST(COUNT(s.shipper_id) AS DOUBLE) AS total_online_hour
        ,SUM(s.work_hour)/CAST(COUNT(s.shipper_id) AS DOUBLE) AS total_work_hour
        ,SUM(s.driver_income)/CAST(COUNT(s.shipper_id) AS DOUBLE) AS total_driver_income
        ,COUNT(s.shipper_id)/COUNT(DISTINCT report_date) AS active_driver

FROM summary s 

INNER JOIN params_date p 
    on s.report_date between CAST(p.start_date AS date) AND CAST(p.end_date AS date)

WHERE p.period_group = '3. Monthly'
GROUP BY 1,2,3,4
;
-- hub
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
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(1 as double)

from raw_date
group by 1,2,3,4,5

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as varchar)
        ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
        ,CAST(date_diff('day',date_trunc('week',report_date),((date_trunc('week',report_date) + interval '7' day))) as double)

from raw_date

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,CAST(date_trunc('month',report_date) as varchar)
        ,CAST((date_trunc('month',report_date) + interval '1' month - interval '1' day) as varchar) 
        ,CAST(date_diff('day',date_trunc('month',report_date),(date_trunc('month',report_date) + interval '1' month)) as double)

from raw_date
)
,final AS 
(SELECT 
         raw.date_ AS report_date
        ,raw.uid AS shipper_id
        ,city_name 
        ,ARRAY_AGG(raw.hub_type_original) AS hub_type_agg
        ,CARDINALITY(ARRAY_AGG(raw.hub_type_original)) AS check_hub
        ,SUM(raw.total_order) AS total_order
        ,SUM(raw.in_shift_online_time) AS online_hour
        ,SUM(raw.in_shift_work_time) AS work_hour
        ,SUM(raw.total_income) AS total_income
        -- ,ROW_NUMBER()OVER(PARTITION BY uid order by slot_id desc) as rank

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics raw 

WHERE total_order > 0

GROUP BY 1,2,3
)
,summary AS 
(SELECT 
         report_date
        ,city_name
        ,CASE WHEN check_hub > 1 THEN 'Multi Shift' ELSE ARRAY_JOIN(hub_type_agg,',') END AS hub_type  
        ,SUM(total_order) AS total_order
        ,SUM(online_hour) AS online_hour
        ,SUM(work_hour) AS work_hour
        ,SUM(total_income) AS total_income
        ,COUNT(DISTINCT shipper_id) AS active

FROM final

GROUP BY 1,2,3
)
SELECT 
         p.period 
        ,p.period_group
        ,city_name
        ,hub_type
        ,SUM(total_order)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_ado
        ,SUM(s.online_hour)/CAST(SUM(s.active) AS DOUBLE) AS total_online_hour
        ,SUM(s.work_hour)/CAST(SUM(s.active) AS DOUBLE) AS total_work_hour
        ,SUM(s.total_income)/CAST(SUM(s.active) AS DOUBLE) AS total_driver_income
        ,SUM(s.active)/COUNT(DISTINCT report_date) AS active_driver        

FROM summary s 

INNER JOIN params_date p 
    on s.report_date between CAST(p.start_date AS date) AND CAST(p.end_date AS date)

WHERE p.period_group = '3. Monthly'
GROUP BY 1,2,3,4
