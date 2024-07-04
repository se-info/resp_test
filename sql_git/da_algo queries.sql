

-- alter table algo_delay_assign_order_data_vn add column eta double;
-- alter table algo_delay_assign_order_data_vn add column order_to_waybill double;
-- alter table algo_delay_assign_order_data_vn add column waybill_to_assign_accept double;
-- alter table algo_delay_assign_order_data_vn add column assign_accept_to_arrive double;
-- alter table algo_delay_assign_order_data_vn add column arrive_to_pick double;
-- alter table algo_delay_assign_order_data_vn add column pick_to_complete double;
-- alter table algo_delay_assign_order_data_vn add column order_to_confirm double;
-- alter table algo_delay_assign_order_data_vn add column confirm_to_arrive double;
-- alter table algo_delay_assign_order_data_vn add column confirm_to_pick double;
-- alter table algo_delay_assign_order_data_vn add column out_order_flow double;



create table if not exists algo_delay_assign_order_data_vn
(
request_id varchar
,request_time  bigint
,timedt varchar
,order_id bigint
,store_id bigint
,buyer_id bigint
,foody_service_id int
,delivery_distance double
,city_id bigint
,district_id bigint
,order_flow int
,ab_group_id varchar
,ab_version varchar
,model_name varchar
,original_predict_value double
,predict_value double
,t_arrive_customer double
,t_arrive_merchant double
,t_assign double
,t_confirm double
,t_pickup double
,t_prep double
,delay_assign_enable double
,delay_assign_time double

,is_cmd_order int
,is_net int
,ata double
    ,dt varchar
) 
with
(
    partitioned_by = array['dt']
)
;



DELETE FROM algo_delay_assign_order_data_vn where dt='${PRE_DAY}';
insert into algo_delay_assign_order_data_vn
with city AS (
    SELECT city_id, city_name
    FROM(
        -- from driver_service driver_lib/managers/driver_contract_manager.py
        VALUES
        (217, 'HCM')
        , (218, 'HN')
        , (219, 'DAD')
        , (220, 'HP')
        , (221, 'CT')
        , (222, 'DN')
        , (223, 'VT')
        , (228, 'BN')
        , (230, 'BD')
        , (238, 'DP')
        , (248, 'KH')
        , (254, 'LD')
        , (257, 'NA')
        , (263, 'QN')
        , (265, 'QNI')
        , (271, 'TN')
        , (273, 'TTH')
        , (281, 'DL')
    ) AS a (city_id, city_name)
),
order_type AS (
    SELECT order_type, order_type_name
    FROM(
        VALUES
        (-1, 'DEFAULT')
        , (0, 'DELIVERY')
        , (1, 'EXPRESS')
        , (2, 'SHOPEE')
        , (3, 'NOWMOTO')
        , (4, 'NOWSHIP')
        , (5, 'NOWSHIP_MERCHANT')
        , (6, 'NOWSHIP_SHOPEE')
        , (7, 'NOWSHIP_SAME_DAY')
        , (8, 'NOWSHIP_MULTI_DROP')
        , (200, 'GROUP')
    ) AS a (order_type, order_type_name)
),
tags AS (
    SELECT *
    FROM(
        VALUES
        -------------------------------------------------------------------------
        ('status', 7, 'DELIVERED')
        , ('status', 8, 'CANCEL')
        , ('status', 9, 'QUIT')
        -------------------------------------------------------------------------
        , ('cancel', 0, 'DEFAULT')
        , ('cancel', 1, 'ADMIN')
        , ('cancel', 2, 'USER')
        , ('cancel', 3, 'MERCHANT')
        , ('cancel', 4, 'CANNOT_PAY_TO_MERCHANT')
        , ('cancel', 5, 'SYSTEM')
        , ('cancel', 6, 'FRAUD_ADMIN')
        -------------------------------------------------------------------------
        , ('assign type', 0, 'UNDEFINED')
        , ('assign type', 1, 'SINGLE_ASSIGN')
        , ('assign type', 2, 'MULTI_ASSIGN')
        , ('assign type', 3, 'STACK_ASSIGN')
        , ('assign type', 4, 'OFFLINE_MULTI_ASSIGN')
        , ('assign type', 5, 'FREE_PICK')
        , ('assign type', 6, 'MANUAL')
        , ('assign type', 7, 'DS_STACK_SINGLE_ASSIGN')
        , ('assign type', 8, 'DS_STACK_MULTI_ASSIGN')
        -------------------------------------------------------------------------
        , ('driver status', 1, 'ASSIGNED')
        , ('driver status', 2, 'DENIED')
        , ('driver status', 3, 'SHIPPER_INCHARGED')
        , ('driver status', 4, 'AUTO_INCHARGED')
        , ('driver status', 5, 'REVERTED_INCHARGED')
        , ('driver status', 6, 'OTHER_SHIPPER_INCHARGED')
        , ('driver status', 7, 'NOT_RECEIVED_PUSH')
        , ('driver status', 8, 'SYSTEM_IGNORE')
        , ('driver status', 9, 'SHIPPER_IGNORE')
        , ('driver status', 10, 'DENIED_DRAFT_TIMED_OUT')
        , ('driver status', 11, 'CANCELED')
        , ('driver status', 12, 'SHIPPER_INCHARGED_TIMEOUT')
        -------------------------------------------------------------------------
        , ('driver order status', 1, 'PENDING')
        , ('driver order status', 2, 'READY_FOR_ASSIGNING')
        , ('driver order status', 100, 'ASSIGNING')
        , ('driver order status', 101, 'ASSIGNING_MANUAL')
        , ('driver order status', 102, 'ASSIGNING_TIMEOUT')
        , ('driver order status', 200, 'COLLECTING')
        , ('driver order status', 201, 'DELIVERING')
        , ('driver order status', 202, 'DELIVERING_RETRY')
        , ('driver order status', 203, 'RETURNING')
        , ('driver order status', 204, 'RETURNING_TO_HUB')
        , ('driver order status', 300, 'DENIED')
        , ('driver order status', 301, 'COLLECTING_FAILED')
        , ('driver order status', 400, 'DELIVERED')
        , ('driver order status', 401, 'QUIT')
        , ('driver order status', 402, 'USER_CANCELLED')
        , ('driver order status', 403, 'SYSTEM_CANCELLED')
        , ('driver order status', 404, 'EXTERNAL_CANCELLED')
        , ('driver order status', 405, 'RETURN_SUCCESS')
        , ('driver order status', 406, 'RETURN_FAILED')
        , ('driver order status', 407, 'RETURN_TO_HUB')
        , ('driver order status', 408, 'RECLAIMED')
        -------------------------------------------------------------------------
        , ('assign status', 1, 'ASSIGNED')
        , ('assign status', 2, 'DENIED')
        , ('assign status', 3, 'SHIPPER_INCHARGED')
        , ('assign status', 4, 'AUTO_INCHARGED')
        , ('assign status', 5, 'REVERTED_INCHARGED')
        , ('assign status', 6, 'OTHER_SHIPPER_INCHARGED')
        , ('assign status', 7, 'NOT_RECEIVED_PUSH')
        , ('assign status', 8, 'SYSTEM_IGNORE')
        , ('assign status', 9, 'SHIPPER_IGNORE')
        , ('assign status', 10, 'DENIED_DRAFT_TIMED_OUT')
        , ('assign status', 11, 'CANCELED')
        , ('assign status', 12, 'SHIPPER_INCHARGED_TIMEOUT') --Deprecated
        , ('assign status', 13, 'SHIPPER_CHECKOUT')
        , ('assign status', 14, 'DENIED_CUSTOMER_FAULT')
        , ('assign status', 15, 'DENIED_MERCHANT_CHANGE_PICK_TIME')
        , ('assign status', 16, 'SHIPPER_INCHARGED_ERROR')
        -------------------------------------------------------------------------
        , ('order status', 1, 'DRAFT')
        , ('order status', 2, 'RECEIVED')
        , ('order status', 3, 'PROCESSING')
        , ('order status', 4, 'VERIFIED')
        , ('order status', 5, 'ASSIGNED')
        , ('order status', 6, 'PICKED')
        , ('order status', 7, 'DELIVERED')
        , ('order status', 8, 'CANCEL')
        , ('order status', 9, 'QUIT')
        , ('order status', 10, 'REASSIGNED')
        , ('order status', 11, 'IN_CHARGED')
        , ('order status', 12, 'DENIED')
        , ('order status', 13, 'CONFIRMED')
        , ('order status', 14, 'M_ASSIGNED')
        , ('order status', 15, 'M_RECEIVED')
        , ('order status', 16, 'M_TIMEOUT')
        , ('order status', 17, 'M_OUT_OF_SERVICE')
        , ('order status', 18, 'M_COOKED')
        , ('order status', 19, 'M_CHANGED')
        , ('order status', 20, 'CAN_NOT_CONNECT')
        , ('order status', 21, 'AUTO_ASSIGN')
        , ('order status', 22, 'AUTO_ASSIGN_TIMEOUT')
        , ('order status', 23, 'PENDING')
        , ('order status', 24, 'REJECTED')
        , ('order status', 25, 'FAILED')
        , ('order status', 26, 'CLOSE')
        , ('order status', 100, 'CLIENT_PICK_ARRIVED') --Virtual  statuses
        , ('order status', 101, 'CLIENT_DROP_ARRIVED')
        -------------------------------------------------------------------------
        --, ('order status', , '')
        -------------------------------------------------------------------------
        , ('note ids', 62, '[EN] Shop is closed')
        , ('note ids', 63, '[EN] Customer could not use discount')
        , ('note ids', 64, '[EN] Delivery time was not correct')
        , ('note ids', 66, '[EN] Customer placed wrong order')
        , ('note ids', 67, '[EN] Customer wanted to change payment method')
        , ('note ids', 68, '[EN] Customer wanted to cancel order')
        , ('note ids', 69, '[EN] Other reason')
        , ('note ids', 70, '[EN] Không có tài xế nhận giao hàng')
        , ('note ids', 73, '[EN] Driver could not continue to deliver')
        , ('note ids', 74, '[EN] Driver could not find merchant/customer')
        , ('note ids', 75, '[EN] Driver did not follow process')
        , ('note ids', 77, '[EN] Drivers attitude was bad')
        , ('note ids', 78, '[EN] Other reason')
        , ('note ids', 79, '[EN] Driver reported Merchant closed Ban')
        , ('note ids', 80, '[EN] Shop was busy')
        , ('note ids', 81, '')
        , ('note ids', 82, '[VI] Wrong price')
        , ('note ids', 83, '')
        , ('note ids', 84, '[EN] Shop is closeddd')
        , ('note ids', 85, '[EN] Power outage shop does not accept orders')
        , ('note ids', 87, '[EN] Payment failed')
        , ('note ids', 88, '[EN] I am busy and cannot receive order')
        , ('note ids', 92, '[EN] Payment is failed')
        , ('note ids', 93, '[EN] Out of stock of all order items')
        , ('note ids', 94, '')
        , ('note ids', 95, '')
        , ('note ids', 97, '')
        , ('note ids', 100, '[EN] Driver reported Merchant closed')
        , ('note ids', 102, '[EN] No Driver found')
        , ('note ids', 104, '[EN] I am busy and cannot receive order')
        , ('note ids', 105, '[EN] I want to change item/Merchant')
        , ('note ids', 106, '[EN] I want to change delivery time')
        , ('note ids', 108, '[EN] I forgot inputting the discount code')
        , ('note ids', 110, '[EN]  I want to change payment method')
        , ('note ids', 111, '[EN] I make duplicate orders')
        , ('note ids', 112, '[EN] Too high item price/shipping fees')
        , ('note ids', 113, '[EN] Pending payment status from bank')
        , ('note ids', 114, '[EN] Incomplete payment process')
        , ('note ids', 130, '[EN] Cannot update payment status')
        , ('note ids', 138, '')
    ) AS a (tag, id, name)
),
flow_tb as (
    select tb.ref_order_id as order_id 
           ,case when ta.order_flow=1 then 'dff' when ta.order_flow=0 then 'mff' else 'none' end as order_flow
    from 
        (select distinct id, order_id
        ,cast(json_extract(order_data, '$.delivery.order_flow') as int) as order_flow
        from shopeefood.shopeefood_mart_dwd_vn_foody_partner_db_driver_order_extra_tab_di
        where cast(dt as varchar)='${PRE_DAY_LINE}'
        ) ta
        left join 
        (select id, ref_order_id from 
            shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live
        where cast(from_unixtime(create_time) as varchar) = '${PRE_DAY_LINE} 00:00:00'
        ) tb
        on ta.order_id = tb.id
),
order_tab as (
    select 
        date_format(from_unixtime(submit_time, 7, 0), '%Y-%m-%d') as dtt
        , json_extract_scalar(extra_data, '$.note_ids') AS note_ids
        , if(final_delivered_time > 0, 1, 0) as complete
        , * 
        ,(final_delivered_time-submit_time) as ata0
        ,(estimated_delivered_time-submit_time) as eta0
    from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live
    where date_format(from_unixtime(submit_time, 7, 0), '%Y-%m-%d') = '${PRE_DAY_LINE}'
        and final_delivered_time > 0
),

driver_order_tab as (
    select date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') as dtt
        , concat(cast(order_status as varchar), '-', d.name) as driver_order_status
        , t.* 
    from shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live as t
    left join tags as d on t.order_status=d.id and d.tag='driver order status'
),

status_tab as (
    select date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d %H:%i:%s') as dtt_
        , concat(cast(t.status as varchar), '-', a.name) as status_name
        , t.*
    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live as t
    left join tags as a on t.status=a.id and a.tag='order status'
),

assign_tab as(
    select date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') as dtt
         ,t.assign_type as assign_type_id
        , concat(cast(t.assign_type as varchar), '-', a1.name) as assign_type_name
        , concat(cast(t.status as varchar), '-', a2.name) as status_name
        , t.*
    from (
        SELECT id,order_id,order_type,city_id,district_id,shipper_uid,assign_type,status,location,experiment_group,extra_data,create_time,update_time,expiry_time 
        FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        UNION ALL
        SELECT id,order_id,order_type,city_id,district_id,shipper_uid,assign_type,status,location,experiment_group,extra_data,create_time,update_time,expiry_time 
        FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    ) as t
    left join tags as a1 on t.assign_type=a1.id and a1.tag='assign type'
    left join tags as a2 on t.status=a2.id and a2.tag='driver status'
),

assign_dedup_tab as (
        select 
            order_id
            ,order_type
            ,min(create_time) as assign_accept_time 
        from assign_tab 
        where status = 3 -- 指派接受
        group by 1,2
),

order_dwd as (
    select o.id as order_id 
        ,o.restaurant_id
        ,o.submit_time as order_create_time -- 订单创建
        ,d.id as waybill_id 
        ,d.create_time as waybill_create_time -- 运单创建
        ,s.paid_time
        ,a.assign_accept_time -- 骑手接单时间
        ,m.confirm_time -- 商户确认时间
        ,d.arrive_time -- 到达商户
        ,d.real_pick_time -- 取餐时间
        ,o.final_delivered_time as order_completed_time -- 完成 
        ,d.driver_order_status
        ,fl.order_flow

        ,o.complete
        ,o.cancel_type
        ,o.is_asap
        ,o.eta0
        ,o.ata0

    from order_tab as o
    left join driver_order_tab as d 
    on o.id = d.ref_order_id and d.ref_order_category=0
    left join 
        (select order_id
                ,min(create_time) as paid_time 
        from status_tab 
        where status = 2 
        group by order_id) as s 
    on o.id=s.order_id
    left join 
        (select order_id
                ,min(create_time) as confirm_time 
        from status_tab 
        where status = 13
        group by order_id) as m
    on o.id=m.order_id
    left join 
        (select *
        from assign_dedup_tab) as a 
    on o.id=a.order_id and a.order_type=0
    left join flow_tb as fl 
    on o.id = fl.order_id
   
),

ata_dwd as (
select *
from(
    select ta.*
        ,order_completed_time - order_create_time as ata 
        ,waybill_create_time_new - order_create_time as order_to_waybill
        ,assign_accept_time - waybill_create_time_new as waybill_to_assign_accept 
        ,arrive_time - assign_accept_time as assign_accept_to_arrive
        ,real_pick_time - arrive_time as arrive_to_pick
        ,order_completed_time - real_pick_time as pick_to_complete
        ,confirm_time - order_create_time as order_to_confirm
        ,arrive_time - confirm_time as confirm_to_arrive
        ,real_pick_time - confirm_time as confirm_to_pick

    from(
        select 
            *
            ,case when waybill_create_time >= paid_time then waybill_create_time
                when waybill_create_time < paid_time then paid_time
                else null end as waybill_create_time_new
        from order_dwd
        ) ta 
    )
),


















query_tb as (
select 
cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.api') as varchar) as api
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.timestamp') as bigint) as request_time
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.request_id') as varchar) as request_id
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.request') as varchar) as request
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.response') as varchar) as response
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.ab_info') as varchar) as ab_info
,cast(json_extract(cast(json_extract(value, '$.content') as varchar), '$.predict_details') as varchar) as predict_details
,*
 from shopeefood_assignment.foodalgo_predict_general_live__vn_continuous_s0_live
where dt = '${PRE_DAY_LINE}'
), order_flow_tb as (
    select 
    api
    ,request_time 
    ,request_id
    ,cast(json_extract(request, '$.order_id') as bigint) as order_id
    ,cast(json_extract(request, '$.store_id') as bigint) as store_id
    ,cast(json_extract(request, '$.buyer_id') as bigint) as buyer_id
    ,cast(json_extract(request, '$.foody_service_id') as int) as foody_service_id
    ,cast(json_extract(request, '$.delivery_distance') as int) as delivery_distance
    ,cast(json_extract(request, '$.store_location.city_id') as int) as city_id
    ,cast(json_extract(request, '$.store_location.district_id') as int) as district_id
    
    ,cast(json_extract(response, '$.order_flow_type') as int) as order_flow
    ,cast(json_extract(ab_info, '$.order_flow.group_id') as varchar) as ab_group_id
    ,cast(json_extract(ab_info, '$.order_flow.version') as varchar) as ab_version
    
    ,cast(json_extract(predict_details, '$.model_name') as varchar) as model_name
    ,cast(json_extract(predict_details, '$.original_predict_value') as double) as original_predict_value
    ,cast(json_extract(predict_details, '$.predict_value') as double) as predict_value

    from query_tb
    where api = 'get_order_flow'
), delay_time_tb as (
    select 

        api
        ,request_time 
        ,request_id

        ,cast(json_extract(request, '$.order_id') as bigint) as order_id
        ,cast(json_extract(request, '$.estimate_time_details.t_arrive_customer.prediction') as double) as t_arrive_customer
        ,cast(json_extract(request, '$.estimate_time_details.t_arrive_merchant.prediction') as double) as t_arrive_merchant
        ,cast(json_extract(request, '$.estimate_time_details.t_assign.prediction') as double) as t_assign
        ,cast(json_extract(request, '$.estimate_time_details.t_confirm.prediction') as double) as t_confirm
        ,cast(json_extract(request, '$.estimate_time_details.t_pickup.prediction') as double) as t_pickup
        ,cast(json_extract(request, '$.estimate_time_details.t_prep.prediction') as double) as t_prep

        ,cast(json_extract(response, '$.delay_assign_enable') as int) as delay_assign_enable
        ,cast(json_extract(response, '$.delay_assign_time') as double) as delay_assign_time

    from query_tb 
    where api = 'get_delay_assign_time'
), label_tb as 
(
    select 'VN' as region
       ,order_id
       ,restaurant_id
       ,shipper_uid
       ,is_net
       ,is_delivered
       ,case when is_cancel = 1 and is_caused_by_driver = 1 then 1 else 0 end as is_cmd_order
       ,city_id
       ,city_name
   from shopeefood.shopeefood_mart_dws_vn_order_di
   where cast(dt as varchar) = '${PRE_DAY_LINE}'
), order_tb as (
        select 
        cast(id as bigint) as order_id
        ,case when final_delivered_time>0 then final_delivered_time - submit_time else null end as ata
    from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live
    where date_format(from_unixtime(submit_time, 7, 0), '%Y-%m-%d') ='${PRE_DAY_LINE}'
)
select 
tt1.request_id
,tt1.request_time 
,date_format(from_unixtime(tt1.request_time/1000, 7, 0), '%Y-%m-%d %H:%i:%s') as timedt
,tt1.order_id
,tt1.store_id
,tt1.buyer_id
,tt1.foody_service_id
,tt1.delivery_distance
,tt1.city_id
,tt1.district_id
,tt1.order_flow
,tt1.ab_group_id
,tt1.ab_version
,tt1.model_name
,tt1.original_predict_value
,tt1.predict_value


,tt2.t_arrive_customer
,tt2.t_arrive_merchant
,tt2.t_assign
,tt2.t_confirm
,tt2.t_pickup
,tt2.t_prep
,tt2.delay_assign_enable
,tt2.delay_assign_time

,lb.is_cmd_order
,lb.is_net
,ob.ata

,ata_dwd.eta0 
,ata_dwd.order_to_waybill
,ata_dwd.waybill_to_assign_accept 
,ata_dwd.assign_accept_to_arrive
,ata_dwd.arrive_to_pick
,ata_dwd.pick_to_complete
,ata_dwd.order_to_confirm
,ata_dwd.confirm_to_arrive
,ata_dwd.confirm_to_pick
,cast(ata_dwd.order_flow  as double) as out_order_flow

,'${PRE_DAY}' as dt
from order_flow_tb tt1 
left join delay_time_tb tt2
on tt1.order_id = tt2.order_id and abs(tt1.request_time-tt2.request_time) < 10000
left join label_tb lb 
on tt1.order_id = lb.order_id
left join order_tb ob 
on tt1.order_id = ob.order_id
left join ata_dwd 
on tt1.order_id = ata_dwd.order_id

;


