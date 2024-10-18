select 
        month_,
        city_name_full,
        sum(total_order)*1.0000/count(distinct (partner_id,date_)) as driver_ado,
        sum(total_earning_before_tax)*1.0000/count(distinct (partner_id,date_)) as daily_income

from
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
        dp.total_order
        

FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab ic

LEFT JOIN dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp ON dp.shipper_id = ic.partner_id AND dp.report_date = ic.date_

WHERE ic.date_ >= date'2024-10-01'
)
group by 1,2