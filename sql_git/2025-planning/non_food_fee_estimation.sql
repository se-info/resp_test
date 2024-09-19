WITH driver_cost_base as 
(select 
    bf.*             
    ,(driver_cost_base + return_fee_share_basic) as dr_cost_base
    ,(driver_cost_surge + return_fee_share_surge) as dr_cost_surge
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee
    ,r.eta_drop_time
    ,r.delivered_timestamp

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

left join driver_ops_raw_order_tab r on r.id = bf.order_id and r.order_type = bf.ref_order_category
                                        
WHERE bf.grass_date != date'2023-06-06'
AND bf.ref_order_category = 0
and bf.status = 7
)
,raw as 
(select 
        bf.grass_date,
        bf.order_id as id,
        coalesce(bf.group_id,0) as group_id,
        bf.city_name_full as city_name,
        bf.partner_id as shipper_id,
        di.name_en as district_name,
        hour(bf.delivered_timestamp) as report_hour,
        cast(ds.min_fee_opt1 as bigint) as min_fee_opt1,
        cast(ds.surge_rate_opt1 as double) as surge_rate_opt1,
        cast(ds.surge_rate_opt2 as double) as surge_rate_opt2,
        cast(ds.surge_rate_opt3 as double) as surge_rate_opt3,
        cast(ds.base_unit_fee as bigint) as base_unit_fee,
        if(delivered_by='hub',1,0) as is_hub_order,
        bf.distance, 
        case when min_fee_opt1 is not null and delivered_by != 'hub' and coalesce(bf.group_id,0) = 0 then 1 else 0 end as is_impact,
        bf.dr_cost_surge + bf.dr_cost_base as current_base_surge

from  driver_cost_base bf 

left join driver_ops_surge_fee_ingest ds 
        on (case when bf.pick_city_id != 219 
        then (cast(ds.district_id as bigint) = bf.pick_district_id and cast(ds.city_id as bigint) = bf.pick_city_id) 
        else cast(ds.city_id as bigint) = bf.pick_city_id end)
        and hour(bf.eta_drop_time)*100 + minute(bf.eta_drop_time) between cast(ds.start_time as bigint) and cast(ds.end_time as bigint)
        and lower(date_format(bf.eta_drop_time,'%W')) = lower(ds.weekday)

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = bf.pick_district_id and di.province_id = bf.pick_city_id

where 1 = 1 
and bf.grass_date between date'2024-08-12' and date'2024-08-18' 
and bf.pick_city_id in (230,219,218,220,223)
)
select * from raw where is_impact = 1 and city_name = 'Vung Tau' and grass_date = date'2024-08-14'
,s as 
(select  
        grass_date,
        report_hour,
        group_id,
        id,
        city_name,
        district_name,
        is_hub_order,
        distance,
        is_impact,
        current_base_surge as ship_fee_current,
        case 
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id = 0 then greatest(min_fee_opt1,distance*base_unit_fee*surge_rate_opt1)
        when min_fee_opt1 is null and is_hub_order = 0 and group_id = 0 then current_base_surge
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id > 0 then current_base_surge
        when min_fee_opt1 is null and is_hub_order = 0 and group_id > 0 then current_base_surge

        when is_hub_order = 1 and group_id = 0 then current_base_surge
        when is_hub_order = 1 and group_id > 0 then current_base_surge
        else current_base_surge end as shipping_fee_opt1,

        case 
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id = 0 then greatest(min_fee_opt1,distance*base_unit_fee*surge_rate_opt2)
        when min_fee_opt1 is null and is_hub_order = 0 and group_id = 0 then current_base_surge
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id > 0 then current_base_surge
        when min_fee_opt1 is null and is_hub_order = 0 and group_id > 0 then current_base_surge

        when is_hub_order = 1 and group_id = 0 then current_base_surge
        when is_hub_order = 1 and group_id > 0 then current_base_surge
        else current_base_surge end as shipping_fee_opt2,

        case 
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id = 0 then greatest(min_fee_opt1,distance*base_unit_fee*surge_rate_opt3)
        when min_fee_opt1 is null and is_hub_order = 0 and group_id = 0 then current_base_surge
        when min_fee_opt1 is not null and is_hub_order = 0 and group_id > 0 then current_base_surge
        when min_fee_opt1 is null and is_hub_order = 0 and group_id > 0 then current_base_surge

        when is_hub_order = 1 and group_id = 0 then current_base_surge
        when is_hub_order = 1 and group_id > 0 then current_base_surge
        else current_base_surge end as shipping_fee_opt3,
        current_base_surge,
        surge_rate_opt1,
        surge_rate_opt2,
        surge_rate_opt3

from raw 

where 1 = 1
)

select
        grass_date,
        city_name,
        -- report_hour,
        count(distinct id) as total_order,
        count(distinct case when group_id > 0 then id else null end) as stacked,
        count(distinct case when is_hub_order = 1 then id else null end) as hub_order,
        count(distinct case when is_hub_order = 1 and group_id > 0 then id else null end) as stacked_hub,
        sum(ship_fee_current) as current_shipping_fee,
        sum(shipping_fee_opt1) as shipping_fee_opt1,
        sum(shipping_fee_opt2) as shipping_fee_opt2,
        sum(shipping_fee_opt3) as shipping_fee_opt3,
        count(distinct case when shipping_fee_opt1 != ship_fee_current then id else null end) as cnt_order_opt1,
        count(distinct case when shipping_fee_opt2 != ship_fee_current then id else null end) as cnt_order_opt2,
        count(distinct case when shipping_fee_opt3 != ship_fee_current then id else null end) as cnt_order_opt3,
        array_agg(distinct case when surge_rate_opt1 is not null then surge_rate_opt1 end) as surge_rate_opt1,
        array_agg(distinct case when surge_rate_opt2 is not null then surge_rate_opt2 end) as surge_rate_opt2,
        array_agg(distinct case when surge_rate_opt3 is not null then surge_rate_opt3 end) as surge_rate_opt3

from s 
-- where city_name = 'Da Nang City'
group by 1,2
