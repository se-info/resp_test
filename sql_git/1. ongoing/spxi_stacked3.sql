with order_base as
(select 
    dot.ref_order_code
    ,bf.order_id
    ,bf.partner_id
    ,bf.group_id
    ,ns.last_incharge_timestamp
    ,ns.picked_timestamp
    ,dot.pick_latitude
    ,dot.pick_longitude
    ,dot.drop_latitude
    ,dot.drop_longitude
    ,bf.distance
    ,bf.distance_grp
    ,bf.distance_all
    ,ns.created_timestamp
    ,bf.order_in_groups
    ,bf.is_stack_group_order
    ,rank() over (partition by bf.group_id, bf.partner_id order by ns.created_timestamp asc) as rnk_order
    ,bf.grass_date
    
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on bf.order_id = dot.ref_order_id and dot.ref_order_category = 6
left join vnfdbi_opsndrivers.ns_performance_tab ns
    on dot.ref_order_code = ns.order_code
where 1=1
and bf.source = 'Now Ship Shopee'
and bf.grass_date between date '2024-03-01'  and date '2024-03-31'
-- and bf.order_in_groups = 2
-- and bf.partner_id = 5164552
)
-- select 
--     *
-- from order_base
-- limit 100

,group_order_base as
(select 
    grass_date
    ,partner_id
    ,group_id
    ,min(last_incharge_timestamp) as min_time
    ,max(picked_timestamp) as max_time
from order_base
where 1=1
and order_in_groups = 2
and is_stack_group_order in (1,2)
group by 1,2,3
)
,driver_location as
(select 
    g.grass_date
    ,g.driver_now_id
    ,hour(ping_time)*100 + minute(ping_time)/3*3 as time_slot
    ,min_by(g.latitude,hour(ping_time)*100 + minute(ping_time)/3*3) as latitude
    ,min_by(g.longitude,hour(ping_time)*100 + minute(ping_time)/3*3) as longitude
    ,min_by(g.ping_time,hour(ping_time)*100 + minute(ping_time)/3*3) as ping_time
from dev_vnfdbi_opsndrivers.bnp_bi_fraud__fgps__driver_track_ping_di g 
-- where driver_now_id = 5164552
group by 1,2,3
)
,stack_model as
(select 
    g.*
    ,gps.latitude
    ,gps.longitude
    ,gps.ping_time

    ,o1.order_id as a_order_id
    ,o1.pick_latitude as a_pick_latitude
    ,o1.pick_longitude as a_pick_longitude
    ,o1.drop_latitude as a_drop_latitude
    ,o1.drop_longitude as a_drop_longitude

    ,o2.order_id as b_order_id
    ,o2.pick_latitude as b_pick_latitude
    ,o2.pick_longitude as b_pick_longitude
    ,o2.drop_latitude as b_drop_latitude
    ,o2.drop_longitude as b_drop_longitude

    ,ob.order_id as eligible_order_id
    ,ob.pick_latitude as eligible_pick_latitude
    ,ob.pick_longitude as eligible_pick_longitude
    ,ob.drop_latitude as eligible_drop_latitude
    ,ob.drop_longitude as eligible_drop_longitude

    ,great_circle_distance(gps.latitude,gps.longitude,ob.pick_latitude,ob.pick_longitude) distance_driver_to_pick_c --- radius

    ,great_circle_distance(o1.pick_latitude,o1.pick_longitude,o1.drop_latitude,o1.drop_longitude) single_distance_a
    ,great_circle_distance(o2.pick_latitude,o2.pick_longitude,o2.drop_latitude,o2.drop_longitude) single_distance_b
    ,great_circle_distance(ob.pick_latitude,ob.pick_longitude,ob.drop_latitude,ob.drop_longitude) single_distance_c


    ,great_circle_distance(o1.pick_latitude,o1.pick_longitude,o2.pick_latitude,o2.pick_longitude) as pick_a_to_pick_b
    ,great_circle_distance(o2.pick_latitude,o2.pick_longitude,ob.pick_latitude,ob.pick_longitude) as pick_b_to_pick_c
    ,great_circle_distance(ob.pick_latitude,ob.pick_longitude,o1.drop_latitude,o1.drop_longitude) as pick_c_to_drop_a
    ,great_circle_distance(o1.drop_latitude,o1.drop_longitude,o2.drop_latitude,o2.drop_longitude) as drop_a_to_drop_b
    ,great_circle_distance(o2.drop_latitude,o2.drop_longitude,ob.drop_latitude,ob.drop_longitude) as drop_b_to_drop_c

from group_order_base g
left join driver_location gps
    on g.grass_date = gps.grass_date and g.partner_id = gps.driver_now_id and gps.ping_time between g.min_time and g.max_time
left join (select * 
            from order_base 
            where 1=1
            and order_in_groups = 2
            and is_stack_group_order in (1,2)
            and rnk_order = 1
            ) o1
    on g.partner_id = o1.partner_id and g.group_id = o1.group_id
left join (select * 
            from order_base 
            where 1=1
            and order_in_groups = 2
            and is_stack_group_order in (1,2)
            and rnk_order = 2
            ) o2
    on g.partner_id = o1.partner_id and g.group_id = o2.group_id
left join order_base ob
    on g.grass_date = ob.grass_date and ob.order_id != o1.order_id and ob.order_id != o2.order_id and ob.created_timestamp between gps.ping_time and g.max_time
    and great_circle_distance(gps.latitude,gps.longitude,ob.pick_latitude,ob.pick_longitude) <= 1.050 and ob.is_stack_group_order = 0
-- where g.partner_id = 5164552
)
,model_base_v2 as
(select 
    s.*
    ,(pick_a_to_pick_b + pick_b_to_pick_c + pick_c_to_drop_a + drop_a_to_drop_b + drop_b_to_drop_c) as optimal_distance
    ,(single_distance_a + single_distance_b + single_distance_c) as distance_all
    ,(single_distance_a + single_distance_b + single_distance_c) / (pick_a_to_pick_b + pick_b_to_pick_c + pick_c_to_drop_a + drop_a_to_drop_b + drop_b_to_drop_c) as re_
from stack_model s
where (single_distance_a + single_distance_b + single_distance_c) > (pick_a_to_pick_b + pick_b_to_pick_c + pick_c_to_drop_a + drop_a_to_drop_b + drop_b_to_drop_c)
)
,final_raw as
(select 
    m.grass_date
    ,m.partner_id
    ,m.group_id
    ,m.min_time
    ,m.max_time
    ,m.a_order_id
    ,m.b_order_id
    ,array_agg(distinct m.eligible_order_id) as array_order
    ,count(distinct eligible_order_id) as total_eligible_orders
    -- ,rank() over (partition by eligible_order_id order by re_ desc,ping_time asc) as rnk_
from model_base_v2 m
group by 1,2,3,4,5,6,7
)
,result as
(select 
    ob.group_id
    ,ob.partner_id
    ,ob.order_in_groups
    ,ob.is_stack_group_order
    ,ob.grass_date
    -- ,f.*
    ,case when f.group_id is not null then 1 else 0 end as is_eligible
from order_base ob
left join final_raw f
    on ob.group_id = f.group_id and ob.grass_date = f.grass_date
where 1=1
and order_in_groups >= 2
and is_stack_group_order in (1,2)
)
,spx_orders as
(
    select 
        grass_date
        ,count(distinct order_id) as total_orders
    from order_base
    group by 1
)


select 
    r.grass_date
    ,r.order_in_groups
    ,r.is_stack_group_order
    ,s.total_orders
    ,count(distinct r.group_id) as total_group
    ,count(distinct case when r.is_eligible = 1 then r.group_id else null end) as total_group_eligible
from result r
left join spx_orders s
    on r.grass_date = s.grass_date
where is_stack_group_order = 2 --- stack
group by 1,2,3,4


