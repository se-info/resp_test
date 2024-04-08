with driver_cost_base as 
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
    -- ,from_unixtime(oct.final_delivered_time-3600) as delivered_timestamp
    -- ,hour(from_unixtime(oct.final_delivered_time-3600)) as delivered_hour
    ,case when m.order_id is not null then 1 else 0 end as is_hub_surge_campaign
    ,coalesce(m.diff,0) as surge_fee_hub_cp
    ,dotet.total_shipping_fee
    ,dotet.unit_fee
    ,dotet.min_fee
    ,dotet.surge_rate
    ,case 
        when bf.city_name in ('HCM', 'HN') then 13500 
        when bf.city_name in ('HP') then 12000
        else  dotet.min_fee end as min_fee_normal


from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc m
    on bf.order_id = m.order_id
left JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0 and dot.submitted_time > 1609439493
left join (SELECT order_id
                ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
            
            from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
            
            )dotet on dot.id = dotet.order_id
where bf.grass_date >= date '2023-09-01'
and bf.source in ('Food')
)
,raw as
(select 
    d.*
    ,case 
        when delivered_by = 'hub' then 0 
        when unit_fee = 0 then 0
        when delivered_by != 'hub' and is_stack_group_order != 0 then 0
        else greatest(min_fee,unit_fee * distance * surge_rate) - greatest(min_fee_normal,unit_fee * distance * surge_rate) end as surge_fee_non_hub_cp
    ,case when (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) > 0 then 1 else 0 end as is_surge_pass_through
from driver_cost_base d
)
select 
    grass_date
    ,DATE_TRUNC('month',grass_date) as grass_month
    ,sum(case when is_surge_pass_through = 1 then (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) else 0 end) as surge_buyer
    -- ,sum(case when is_surge_pass_through = 1 then (dr_cost_bw_fee_usd) else 0 end) as surge_buyer_bwf
    -- ,sum(case when is_surge_pass_through = 1 then (dr_cost_late_night_usd) else 0 end) as surge_buyer_lnf
    -- ,sum(case when is_surge_pass_through = 1 then (dr_cost_holiday_fee) else 0 end) as surge_buyer_hldf
    ,sum(case when surge_fee_non_hub_cp > 0 or surge_fee_hub_cp >0 or (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) > 0 then coalesce(surge_fee_non_hub_cp,0)/exchange_rate  + coalesce(surge_fee_hub_cp,0)/exchange_rate +(dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) else 0 end) as surge_driver
    ,count(distinct case when is_surge_pass_through = 1 then order_id else null end) as order_surge_buyer
    ,count(distinct case when is_surge_pass_through = 1 and (dr_cost_bw_fee_usd) > 0 then order_id else null end) as order_surge_buyer_bwf
    ,count(distinct case when is_surge_pass_through = 1 and (dr_cost_late_night_usd) > 0 then order_id else null end) as order_surge_buyer_lnf
    ,count(distinct case when is_surge_pass_through = 1 and (dr_cost_holiday_fee) > 0 then order_id else null end) as order_surge_buyer_hldf
    ,count(distinct case when surge_fee_non_hub_cp > 0 or surge_fee_hub_cp >0 or (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) > 0 then order_id  else null end) as order_surge_driver
    ,count(distinct order_id) as total_orders
from raw
where status = 7 --> net order
group by 1,2
