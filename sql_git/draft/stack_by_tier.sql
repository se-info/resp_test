with hub_income as
(select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
)
,final as
(select 
    grass_date
    -- ,bf.partner_id
    ,case 
        when delivered_by = 'hub' and bf.current_driver_tier = 'Hub' then 'hub-inshift'
        when delivered_by != 'hub' and bf.current_driver_tier = 'Hub' then 'hub-outshift'
        when bf.current_driver_tier is null then 'OTH'
        else bf.current_driver_tier end as current_driver_tier
    ,coalesce(case 
                when shift_category_name = '8 hour shift' then 'HUB-08'
                when shift_category_name = '10 hour shift' then 'HUB-10'
                when shift_category_name = '5 hour shift' then 'HUB-05'
                when shift_category_name = '3 hour shift' then 'HUB-03'
                else null end
                ,hub_tab.hub_type_v2) as hub_type
    ,count(distinct case 
        when is_stack_group_order > 0 then order_id
        else null end) as total_order_stack
    ,count(distinct order_id) as total_order
    ,count(distinct case 
        when is_stack_group_order > 0 then order_id
        else null end)*1.0000/count(distinct order_id) as pct_stack
    ,count(distinct partner_id) as total_drivers
    

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join hub_income hub
    on bf.partner_id = hub.shipper_id and bf.grass_date = hub.report_date and bf.current_driver_tier = 'Hub'
left join vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab hub_tab
    on bf.partner_id = hub_tab.shipper_id and bf.grass_date = hub_tab.report_date and bf.current_driver_tier = 'Hub'
where source in ('Food')
and grass_date between current_date - interval '7' day and current_date - interval '1' day
group by 1,2,3
)
select 
    grass_date
    ,current_driver_tier
    ,case when current_driver_tier = 'hub-inshift' then hub_type else null end as hub_type
    ,sum(total_order_stack) total_stacked_orders
    ,sum(total_order) total_del_orders
    ,sum(total_order_stack)*1.0000/sum(total_order) pct_stack
    ,sum(total_drivers) as total_drivers
from final
where case 
    when current_driver_tier in ('hub-intshift','hub-outshift') and hub_type is null then 0 
    when current_driver_tier = 'hub-outshift' and hub_type = 'All-day' then 0
    else 1 end = 1
group by 1,2,3