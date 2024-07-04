with aa as
(select 
        a.uid,
        cast(json_extract(t.setting,'$.order_category') as bigint) as service,
        cast(json_extract(t.setting,'$.is_auto_accept') as boolean) as is_aa

from
(select 
uid,
json_extract(extra_data,'$.ss_setting_by_services') as aa_setting


from shopeefood.foody_partner_db__shipper_settings_tab__reg_daily_s0_live
)a 
cross join unnest (cast(aa_setting as array<json>)) as t(setting)
)
,f as 
(select 
        dp.report_date,
        dp.shipper_id,
        dp.shipper_type,
        dp.city_name,
        dp.total_order,
        case 
        when aa.service = 0 and aa.is_aa = true then 1 else 0 end as is_turn_on_aa_food,

        case 
        when aa.service = 6 and aa.is_aa = true then 1 else 0 end as is_turn_on_aa_shopee,

        case 
        when aa.service not in (0,6) and aa.is_aa = true then 1 else 0 end as is_turn_on_aa_spxi_non_shopee,

        ds.is_available_spxi


from driver_ops_driver_performance_tab dp 

left join aa on aa.uid = dp.shipper_id

left join driver_ops_driver_services_tab ds 
        on ds.report_date = dp.report_date
        and ds.uid = dp.shipper_id

where dp.report_date = date'2024-01-28'
and dp.total_order > 0 )
select 
        report_date,
        case 
        when city_name in ('HCM City','Ha Noi City','Da Nang City') then city_name
        else 'Other' end as city_group,
        IF(shipper_type=12,'hub','non-hub') as type_,
        is_available_spxi,
        count(distinct shipper_id) as a1,
        count(distinct case when is_turn_on_aa_food = 1 then shipper_id else null end) as driver_turn_on_aa_food,
        count(distinct case when is_turn_on_aa_shopee = 1  and shipper_type != 12 then shipper_id else null end) as driver_turn_on_aa_shopee,
        count(distinct case when is_turn_on_aa_spxi_non_shopee = 1  and shipper_type != 12 then shipper_id else null end) as driver_turn_on_aa_spxi_non_shopee

from f 

group by 1,2,3,4

;

-- Order performance
with sa as 
(select 
        ref_order_id,
        order_category,
        count(distinct (ref_order_id,driver_id,create_time)) as no_assign,
        count(distinct case when status in (3,4) then (driver_id,order_code,create_time) else null end) as no_incharged,
        count(distinct case when status in (8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_ignored,
        count(distinct case when status in (2,14,15) then (driver_id,order_code,create_time) else null end) as no_deny,
        max_by(is_auto_accepted,create_time) as is_aa,
        max_by(is_ca,create_time) as is_ca
from
(select 
        *,
        case when experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted,
        case when experiment_group in (5,6,7,8) then 1 else 0 end as is_ca

from driver_ops_order_assign_log_tab
where status in (3,4,2,14,15,8,9,17,18)
)
group by 1,2
)
,f as 
(select 
        r.created_date as created,
        r.id,
        case 
        when r.order_type = 0 then 'food'
        when r.order_type = 6 then 'shopee'
        when r.order_type not in (0,6) then 'spxi_off-shopee' end as source,
        case 
        when r.driver_policy = 2 then 1 else 0 end as is_hub,
        case 
        when r.hub_id > 0 then 1 else 0 end as is_hub_order,
        case 
        when r.group_id > 0 and order_assign_type != 'Group' then 2
        when r.group_id > 0 and order_assign_type = 'Group' then 1 
        else 0 end as assign_type,
        sa.is_aa,
        sa.is_ca,
        sa.no_assign,
        sa.no_ignored,
        sa.no_deny,
        city_group


from driver_ops_raw_order_tab r 

left join sa on sa.ref_order_id = r.id and sa.order_category = r.order_type

where r.order_status in ('Delivered','Returned')
and r.shipper_id > 0 
and r.created_date between current_date - interval '7' day and current_date - interval '1' day
)
select 
        f.created,
        city_group,
        source,
        assign_type,
        is_ca,
        is_hub_order,
        count(distinct id) as ado,  
        sum(no_assign) as total_assign,
        sum(no_ignored) as total_ignored,
        sum(no_deny) as total_deny

from f 

group by 1,2,3,4,5,6


