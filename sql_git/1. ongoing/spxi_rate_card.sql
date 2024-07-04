WITH driver_cost_base as 
(select 
    bf.*
                                                          
                                                                                      
                                              
    ,(driver_cost_base + return_fee_share_basic) as dr_cost_base
    ,(driver_cost_surge + return_fee_share_surge) as dr_cost_surge
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
                                                                                                                   
WHERE grass_date != date'2023-06-06'

)
SELECT  
        ns.created_date,
        ns.order_code,
        ns.distance,
        ns.city_name,
        ns.order_status,
        db.dr_cost_base,
        db.dr_cost_surge,
        fee.user_fee,
        fee.discount_amount

FROM vnfdbi_opsndrivers.ns_performance_tab ns

LEFT JOIN driver_cost_base db ON db.order_id = ns.id AND db.ref_order_category = ns.order_type

LEFT JOIN 
(
select 
        id,
        shipping_fee*1.0000/100 AS user_fee,
        discount_amount AS discount_amount

from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ) fee ON fee.id = ns.id 

WHERE ns.grass_date >= date'2024-05-01'
and ns.grass_date <= date'2024-05-31'
and ns.source != 'now_ship_shopee'
and ns.city_name IN ('HCM City','Ha Noi City')
AND ns.order_status = 'Delivered'