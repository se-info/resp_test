select 
        raw.ref_order_code
        ,raw.shopee_shipping_code
        ,raw.item_value
        ,raw.payment_at_buyer
        ,dot.delivery_distance/cast(1000 as double) as distance_
        ,dot.delivery_cost/cast(100 as double) as driver_fee 


from dev_vnfdbi_opsndrivers.phong_raw_order_checking raw 
left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dot.ref_order_code = raw.ref_order_code and dot.ref_order_category = 6

where shopee_shipping_code in 
('VN220975656212P',
'VN2285113840302',
'VN220232488611G',
'VN223178686960T',
'VN226194837177R',
'VN229800657007N',
'VN220736494947Q',
'VN2209144891808',
'VN221411870134Q',
'VN220532664257O',
'VN222987076611Z',
'VN222881464911A',
'VN229122448316X',
'VN221852105554M',
'VN227635866415C',
'VN222904584179N',
'VN220344717684L',
'VN2287852381534',
'VN226485495709T',
'VN224578904598O',
'VN226186297435R',
'VN227009239987U',
'VN2203652585478',
'VN229941301048E',
'VN228030275051I')
