WITH raw AS
(SELECT 
        DATE_TRUNC('month',ic.date_) AS month_,
        ic.date_,
        ic.current_driver_tier,
        ic.partner_id,
        ic.total_earning_before_tax,
        ic.city_name_full,
        ic.total_bill,
        (ic.total_bill_food + ic.total_bill_market) AS total_delivery_ado,
        (ic.total_bill_now_ship + ic.total_bill_now_ship_shopee + ic.total_bill_now_ship_instant 
                + ic.total_bill_now_ship_food_merchant + ic.total_bill_now_ship_sameday) AS total_spxi_ado,
        GREATEST(dp.online_hour,dp.work_hour) AS online_hour
        

FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab ic

LEFT JOIN dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp ON dp.shipper_id = ic.partner_id AND dp.report_date = ic.date_

WHERE ic.date_ >= DATE'2024-05-01' AND ic.date_ <= DATE'2024-05-31'
)
,tier_tab AS
(SELECT
        month_,
        partner_id,
        MAX_BY(current_driver_tier,working_days) AS most_working_type
FROM
(SELECT 
        month_,
        partner_id,
        current_driver_tier,
        COUNT(DISTINCT date_) AS working_days

FROM raw 

GROUP BY 1,2,3
)
GROUP BY 1,2
)
SELECT 
        raw.month_,
        raw.city_name_full,
        tt.most_working_type,
        COUNT(DISTINCT CASE WHEN raw.total_bill > 0 THEN (raw.date_,raw.partner_id) ELSE NULL END)
              /CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS avg_a1,
        SUM(raw.total_bill)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_ado,
        SUM(raw.total_delivery_ado)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_delivery_ado,
        SUM(raw.total_spxi_ado)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_spxi_ado,
        AVG(online_hour) AS avg_online_hour,
        SUM(total_earning_before_tax)/CAST(COUNT(DISTINCT raw.partner_id) AS DOUBLE) AS monthly_income,
        AVG(total_earning_before_tax) AS daily_income


FROM raw 

LEFT JOIN tier_tab tt ON tt.month_ = raw.month_ AND tt.partner_id = raw.partner_id

GROUP BY 1,2,3


