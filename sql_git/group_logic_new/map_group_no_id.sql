with api_log as
(select
    cast(JSON_EXTRACT(details ,'$.shippingfee_kafka.calculation_detail.parameters.shippingfee_calculate_request.original_delivery_info.delivery_order_ids[0]') as bigint) as hold_order_id
    ,cast(JSON_EXTRACT(details ,'$.shippingfee_kafka.extra_msg.distance_config_request.shippingfee_request.data.assign_info.driver_id') as bigint) as driver_id
    --  ,JSON_EXTRACT(details ,'$.shippingfee_kafka.extra_msg.distance_config_request.shippingfee_request.data.assign_info') as x2
    -- ,case when JSON_EXTRACT(details ,'$.shippingfee_kafka.calculation_detail.parameters.shippingfee_calculate_request.original_delivery_info.delivery_order_ids') is not null then
    --     JSON_EXTRACT(details ,'$.shippingfee_kafka.calculation_detail.parameters.shippingfee_calculate_request.original_delivery_info.delivery_order_ids[1]') else null end
    ,*
from shopeefood_assignment.foodalgo_shippingfee_snapshot__vn_continuous_s0_live
-- where api = 'get_group_driver_shippingfee'
where 1=1
and deal_order_type_txt = 'delivery'
and api = 'get_group_driver_shippingfee'
-- and cast(JSON_EXTRACT(details ,'$.shippingfee_kafka.calculation_detail.parameters.shippingfee_calculate_request.original_delivery_info.delivery_order_ids[0]') as bigint) = 565506226
and group_id = '0'
)
,group_tab as
(select
    ogi.id as group_id
    ,ogi.group_code as group_code
    ,uid as driver_id
    ,ogm.order_id
    ,ogm.ref_order_id
    ,ogm.ref_order_category
from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
    on ogi.id = ogm.group_id
and mapping_status in (11,23,26,13)
)
select
    gt.*
    ,a.*
from api_log a
left join group_tab gt
    on a.hold_order_id = gt.order_id and a.driver_id = gt.driver_id
where gt.group_id = 0