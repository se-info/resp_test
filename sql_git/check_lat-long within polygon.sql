select 
    t1.*
    ,t2.district_id
from shopeefood.foody_delivery_db__shipping_info_tab__reg_daily_s0_live t1
left join vnfdbi_opsndrivers.shopeefood_bnp_district_polygon_dim_tab t2
    on ST_Within(ST_Point(t1.longitude,t1.latitude ), ST_GeometryFromText(t2.field_coords))
limit 10
;
-- check assign location / hub polygon 
select 
        order_id,
        cast(split(location,',')[1] as double) as lat,
        cast(split(location,',')[2] as double) as long,
        t2.hub_id,
        hi.hub_priority,
        t1.driver_id as shipper_id,
        t1.create_time
from driver_ops_order_assign_log_tab t1 
left join  dev_vnfdbi_opsndrivers.driver_ops_hub_polygon_tab t2
    on ST_Within(ST_Point(cast(split(location,',')[1] as double),cast(split(location,',')[2] as double)), ST_GeometryFromText(t2.hub_polygon))

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi 
   on hi.id = cast(t2.hub_id as bigint)
;
select
    t1.restaurant_id
    ,t1.offers_discount
    ,t1.name
    ,t1.review_count
    ,t1.sort_rating
    ,t1.latitude
    ,t1.longitude
    ,t1.order_mode
    ,t1.address
    ,t1.price_range
    ,t1.status
    ,t1.is_closed
    ,t1.score
    ,t1.is_partner
    ,t1.merchant_id
    ,t1.brand_id
    ,t1.median_price
    ,t1.has_be_point
    ,t1.be_point_campaign
    ,t2.city_id 
    ,try_cast(t3.district_id as decimal) district_id 
    ,coalesce(prov.name_en,prov2.name_en) as province_name
    -- ,dist.province_id
    ,dist.name_en as dist_name
    -- ,prov2.name_en as province_name_v2
from dev_vnfdbi_opsndrivers.shopeefood_bi_be_merchant_crawled t1
left join vnfdbi_opsndrivers.shopeefood_bnp_city_polygon_dim_tab t2 
    on ST_Within(ST_Point(try_cast(t1.longitude as double), try_cast(t1.latitude as double) ), ST_GeometryFromText(t2.field_coords)) 
left join vnfdbi_opsndrivers.shopeefood_bnp_district_polygon_dim_tab t3 
    on ST_Within(ST_Point(try_cast(t1.longitude as double), try_cast(t1.latitude as double) ), ST_GeometryFromText(t3.field_coords)) 
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live prov
    on cast(t2.city_id as decimal) = prov.id and prov.country_id = 86
left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live dist
    on cast(t3.district_id as decimal) = dist.id
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live prov2
    on dist.province_id = prov2.id and prov2.country_id = 86