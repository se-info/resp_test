with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',current_date) - interval '60' day,current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,1

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,raw_order AS 
(SELECT * FROM dev_vnfdbi_opsndrivers.phong_hub_temp_table)
,out_shift as 
(
SELECT 
        report_date
       ,uid AS shipper_id
       ,0 AS registered_
       ,city_name 
       ,CASE
            WHEN shipper_type_id = 12 then 'out-shift'
            ELSE 'non-hub'
            END AS type_
        ,0 AS slot_id    
        ,COUNT(DISTINCT ref_order_code) AS total_order            


FROM raw_order ro 
WHERE is_hub = 0 
GROUP BY 1,2,3,4,5,6
)
,inshift_raw AS 
(
SELECT 
        date_ AS report_date
       ,uid AS shipper_id
       ,registered_ 
       ,city_name
       ,hub_type AS type_
       ,slot_id
       ,COALESCE(total_order,0) AS total_order


FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics

WHERE 1 = 1
AND date_ BETWEEN date_trunc('month',current_date) - interval '60' day AND current_date - interval '1' day

)
,metrics AS 
(
SELECT *

FROM inshift_raw

UNION ALL

SELECT *

FROM out_shift
)
,summary AS 
(SELECT
        report_date
       ,city_name
       ,type_ 
       ,SUM(total_order) AS total_order
       ,COUNT(CASE WHEN registered_= 1 THEN shipper_id ELSE NULL END) AS total_registered
       ,COUNT(CASE WHEN total_order > 0 THEN shipper_id ELSE NULL END) AS total_active

FROM metrics 

GROUP BY 1,2,3

UNION ALL 

SELECT
        report_date
       ,city_name
       ,'All' AS type_ 
       ,SUM(total_order) AS total_order
       ,COUNT(DISTINCT CASE WHEN registered_= 1 THEN shipper_id ELSE NULL END) AS total_registered
       ,COUNT(DISTINCT CASE WHEN total_order > 0 THEN shipper_id ELSE NULL END) AS total_active

FROM metrics 
WHERE type_ not in ('non-hub','out-shift')
GROUP BY 1,2,3
)
SELECT 
        p.period_group
       ,p.period 

       ,SUM(CASE WHEN type_ not in ('non-hub','All') THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_delivered_by_hub
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_in_shift
       ,SUM(CASE WHEN type_ = 'out-shift' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_out_shift       
       ,SUM(CASE WHEN type_ = '10 hour shift' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub10
       ,SUM(CASE WHEN type_ = '8 hour shift' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub8
       ,SUM(CASE WHEN type_ = '5 hour shift' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub5
       ,SUM(CASE WHEN type_ = '3 hour shift' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub3
       ,SUM(CASE WHEN type_ = 'non-hub' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_delivered_by_non_hub
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub_reg
       ,SUM(CASE WHEN type_ = '10 hour shift' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub10_reg
       ,SUM(CASE WHEN type_ = '8 hour shift' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub8_reg
       ,SUM(CASE WHEN type_ = '5 hour shift' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub5_reg
       ,SUM(CASE WHEN type_ = '3 hour shift' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub3_reg
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub_active
       ,SUM(CASE WHEN type_ = '10 hour shift' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub10_active
       ,SUM(CASE WHEN type_ = '8 hour shift' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub8_active
       ,SUM(CASE WHEN type_ = '5 hour shift' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub5_active
       ,SUM(CASE WHEN type_ = '3 hour shift' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_hub3_active
       ,SUM(CASE WHEN type_ = 'All' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS vn_unique_active 


       ,SUM(CASE WHEN type_ not in ('non-hub','All') AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_delivered_by_hub
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_in_shift
       ,SUM(CASE WHEN type_ = 'out-shift' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_out_shift
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub10
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub8
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub5
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub3
       ,SUM(CASE WHEN type_ = 'non-hub' AND city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_delivered_by_non_hub        
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'HCM City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub_reg
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'HCM City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub10_reg
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'HCM City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub8_reg
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'HCM City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub5_reg
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'HCM City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub3_reg
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub_active
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub10_active
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub8_active
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub5_active
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_hub3_active
       ,SUM(CASE WHEN type_ = 'All' AND city_name = 'HCM City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_unique_active 


       ,SUM(CASE WHEN type_ not in ('non-hub','All') AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_delivered_by_hub
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_in_shift
       ,SUM(CASE WHEN type_ = 'out-shift' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_out_shift
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub10
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub8
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub5
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub3
       ,SUM(CASE WHEN type_ = 'non-hub' AND city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_delivered_by_non_hub        
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Ha Noi City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub_reg
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Ha Noi City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub10_reg
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Ha Noi City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub8_reg
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Ha Noi City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub5_reg
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Ha Noi City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub3_reg
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub_active
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub10_active
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub8_active
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub5_active
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_hub3_active
       ,SUM(CASE WHEN type_ = 'All' AND city_name = 'Ha Noi City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hn_unique_active 

       ,SUM(CASE WHEN type_ not in ('non-hub','All') AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_delivered_by_hub
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_in_shift
       ,SUM(CASE WHEN type_ = 'out-shift' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_out_shift
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub10
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub8
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub5
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub3
       ,SUM(CASE WHEN type_ = 'non-hub' AND city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_delivered_by_non_hub   
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Hai Phong City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub_reg
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Hai Phong City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub10_reg
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Hai Phong City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub8_reg
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Hai Phong City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub5_reg
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Hai Phong City' THEN total_registered ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub3_reg
       ,SUM(CASE WHEN type_ not in ('non-hub','All','out-shift') AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub_active
       ,SUM(CASE WHEN type_ = '10 hour shift' AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub10_active
       ,SUM(CASE WHEN type_ = '8 hour shift' AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub8_active
       ,SUM(CASE WHEN type_ = '5 hour shift' AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub5_active
       ,SUM(CASE WHEN type_ = '3 hour shift' AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_hub3_active
       ,SUM(CASE WHEN type_ = 'All' AND city_name = 'Hai Phong City' THEN total_active ELSE NULL END)/CAST(p.days AS DOUBLE) AS hp_unique_active        
       
       ,p.days

FROM summary s 

INNER JOIN params_date p 
    on s.report_date BETWEEN p.start_date and p.end_date

GROUP BY 1,2,p.days    