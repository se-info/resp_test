with district_raw(id,district_name) as 
(VALUES
(1,'Quận 1'),
(2,'Quận Gò Vấp'),
(4,'Quận 2'),
(5,'Quận 3'),
(6,'Quận 4'),
(7,'Quận 5'),
(8,'Quận 6'),
(9,'Quận 7'),
(10,'Quận 8'),
(11,'Quận 9'),
(12,'Quận 10'),
(13,'Quận 11'),
(14,'Quận 12'),
(15,'Quận Bình Thạnh'),
(16,'Quận Tân Bình'),
(17,'Quận Phú Nhuận'),
(18,'Quận Bình Tân'),
(19,'Quận Tân Phú'),
(693,'Thành Phố Thủ Đức'),
(694,'Huyện Củ Chi'),
(695,'Huyện Hóc Môn'),
(696,'Huyện Bình Chánh'),
(698,'Cần Giờ'),
(699,'Huyện Nhà Bè')
)
,raw as 
(select 
        ba.grass_date,
        ba.order_id,
        ba.buyer_shipping_address_city,
        dr.district_name,
        dr.id,
        t3.field_geom_type,
        t3.field_coords

from driver_ops_sbs_instant_buyer_address_sample ba 

left join district_raw  dr on ba.buyer_shipping_address_city = dr.district_name

left join vnfdbi_opsndrivers.shopeefood_bnp_district_polygon_dim_tab t3 on cast(t3.district_id as double) = dr.id
)
select 
        -- grass_date,
        district_name,
        field_coords,
        count(distinct (order_id,grass_date)) as cnt_order

from raw

group by 1,2
;
-- e2c
with pick as 
(select distinct 
        cast(json_extract(extra_data,'$.sender_info.username') as varchar) as sender,
        cast(json_extract(extra_data,'$.sender_info.district_id') as bigint) as sender_district,
        city_id


from shopeefood.foody_express_db__shopee_booking_tab__reg_continuous_s0_live
where date(from_unixtime(create_time - 3600)) between date'2024-08-19' and date'2024-08-28'
and city_id in (217,218)
)
,drop_ as 
(select distinct 
        cast(json_extract(extra_data,'$.recipient_info.name') as varchar) as buyer,
        cast(json_extract(extra_data,'$.recipient_info.district_id') as bigint) as buyer_district,
        city_id


from shopeefood.foody_express_db__shopee_booking_tab__reg_continuous_s0_live
where date(from_unixtime(create_time - 3600)) between date'2024-08-19' and date'2024-08-28'
and city_id in (217,218)
)
select  
        di.name_en as district_name,
        'pick' as "type",
        t3.field_coords,
        count(distinct sender) as cnt_points

from pick p

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = p.sender_district and di.province_id = p.city_id

left join vnfdbi_opsndrivers.shopeefood_bnp_district_polygon_dim_tab t3 on cast(t3.district_id as double) = p.sender_district

group by 1,2,3

UNION ALL 

select  
        di.name_en as district_name,
        'drop' as "type",
        t3.field_coords,
        count(distinct buyer) as cnt_points

from drop_ d

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = d.buyer_district and di.province_id = d.city_id

left join vnfdbi_opsndrivers.shopeefood_bnp_district_polygon_dim_tab t3 on cast(t3.district_id as double) = d.buyer_district

group by 1,2,3
