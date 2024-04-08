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