wITH driver_list AS
(SELECT  
       report_date, 
       uid AS shipper_id,
       service_name,
       CARDINALITY(FILTER(service_name,x ->x in ('Delivery') )) as delivery_service_filter,
       CARDINALITY(FILTER(service_name,x ->x in ('Now Ship','Ship Shopee') )) as ship_service_filter

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_services_tab s 

WHERE 1 = 1
)
,driver_cost_base as 
(select 
    bf.*
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee
    ,case 
        when dl.ship_service_filter > 0 AND delivery_service_filter = 0 then 1 
        else 0 end as is_spxi_only
    ,(CASE
    -- WHEN is_nan(bfo.bonus_v2) = true THEN 0.00
    WHEN bf.delivered_by = 'hub' THEN bonus_hub_v2
    WHEN bf.delivered_by != 'hub' THEN bonus_non_hub_v2
    ELSE null end)*1.000000/exchange_rate   as bonus_usd_all_v2
    , (driver_cost_base_v2 + bf.return_fee_share_basic)*1.000000/exchange_rate   as total_driver_cost_base_all_v2
    , (driver_cost_surge_v2 + bf.return_fee_share_surge)*1.000000/exchange_rate   as total_driver_cost_surge_all_v2
    , case when bf.grass_date <= date '2024-07-31' then bf.is_stack_group_order else god.is_actual_stack_group_order end as is_stack_group_order_new

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on bf.group_id = god.group_id and bf.order_id = god.ref_order_id
left join driver_list dl 
        ON dl.shipper_id = bf.partner_id
        AND dl.report_date = bf.grass_date

where bf.grass_date between date'2024-07-01' AND date'2024-08-31'
and bf.source = 'Now Ship Shopee'
-- and shipper_type_id = 11
)
-- select * from driver_cost_base where is_spxi_only = 1 
select
        date_trunc('month',grass_date) as month_,
        coalesce(is_spxi_only,2) as type_,
        -- case 
        -- when distance <= 5 then '1. 0 - 5km' 
        -- when distance <= 8 then '2. 5 - 8km'
        -- when distance <= 10 then '3. 8 - 10km'
        -- when distance <= 15 then '4. 10 - 15km'
        -- when distance > 15 then '5. ++15km' end as distance_range,
        -- (sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))/count(order_id) as cpo,
        sum(dr_cost_bonus_usd + dr_cost_base_usd + dr_cost_surge_usd)/count( order_id) as driver_cost_v1,
        sum(bonus_usd_all_v2 + total_driver_cost_base_all_v2 + total_driver_cost_surge_all_v2)/count( order_id) as driver_cost_v2,
        count(distinct order_id) as total,
        sum(total_driver_cost_base_all_v2)/count( order_id) as total_driver_cost_base_all_v2,
        sum(total_driver_cost_surge_all_v2)/count( order_id) as total_driver_cost_surge_all_v2,
        sum(bonus_usd_all_v2)/count( order_id) as bonus_usd_all_v2,
        sum(dr_cost_base_usd)/count( order_id) as total_driver_cost_base_all_v1,
        sum(dr_cost_surge_usd)/count( order_id) as total_driver_cost_surge_all_v1,
        sum(dr_cost_bonus_usd)/count( order_id) as bonus_usd_all_v1


from driver_cost_base

group by 1,2