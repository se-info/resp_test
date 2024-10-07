WITH pay_note(week_num,max_date,min_date) as 
(SELECT
        year(report_date)*100 + week(report_date),
        max(report_date),
        min(report_date)
FROM
(
(SELECT SEQUENCE(date_trunc('week',current_date)- interval '2' month,date_trunc('week',current_date) - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
group by 1 
)
,f AS
(SELECT 
       DATE(delivered_timestamp) AS report_date, 
       YEAR(DATE(delivered_timestamp))*100+WEEK(DATE(delivered_timestamp)) AS week_num,
       raw.shipper_id,
       raw.city_name,
       COUNT(DISTINCT CASE WHEN raw.order_type = 6 THEN order_code ELSE NULL END) AS e2c,
       COUNT(DISTINCT CASE WHEN raw.order_type != 6 THEN order_code ELSE NULL END) AS c2c


FROM dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

WHERE 1 = 1 
AND raw.order_type != 0 
AND raw.order_status IN ('Delivered','Returned')
AND DATE(COALESCE(raw.delivered_timestamp,raw.returned_timestamp)) BETWEEN date'${start_date}' and date'${end_date}'
GROUP BY 1,2,3,4
)
,m AS
(SELECT 
        f.*,
        d.online_hour,
        dp.sla_rate

FROM f

LEFT JOIN 
(SELECT 
        created,
        uid,
        SUM(online_by_hour*1.00/3600) AS online_hour 

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_supply_tab 
GROUP BY 1,2
)d ON d.created = f.report_date AND d.uid = f.shipper_id 
LEFT JOIN driver_ops_driver_performance_tab dp ON dp.shipper_id = f.shipper_id and dp.report_date = f.report_date
)
,eligible AS
(SELECT 
        week_num,
        shipper_id,
        city_name,
        SUM(e2c+c2c) AS total_order,
        COUNT(DISTINCT report_date) as working_days,
        COUNT(DISTINCT CASE WHEN sla_rate >= 90 THEN report_date ELSE NULL END) AS qualified_working_days,
        CASE 
        WHEN COUNT(DISTINCT CASE WHEN sla_rate >= 90 THEN report_date ELSE NULL END) >= 4 AND SUM(e2c+c2c) >= 150 then 500000
        WHEN COUNT(DISTINCT CASE WHEN sla_rate >= 90 THEN report_date ELSE NULL END) >= 4 AND SUM(e2c+c2c) >= 110 then 300000
        WHEN COUNT(DISTINCT CASE WHEN sla_rate >= 90 THEN report_date ELSE NULL END) >= 4 AND SUM(e2c+c2c) >= 80 then 200000
        ELSE 0 END AS bonus_value

FROM m 
GROUP BY 1,2,3
)
SELECT 
        el.week_num,
        el.shipper_id,
        dp.shipper_name,
        dp.city_name,
        el.total_order,
        el.working_days,
        el.qualified_working_days,
        el.bonus_value,
        'spf_do_0006|NON-HUB_SPXI_Thuong tai xe guong mau tuan_'||date_format(pn.min_date,'%Y-%m-%d')||' - '|| date_format(pn.max_date,'%Y-%m-%d') as bonus_note

FROM eligible el 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master dp ON dp.shipper_id = el.shipper_id AND dp.grass_date = 'current'

LEFT JOIN pay_note pn 
    on pn.week_num = el.week_num


-- select * from driver_ops_spxi_only_driver_list       