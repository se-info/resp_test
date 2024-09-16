with bill_fee_order_base as
(select 
    b.*
    ,first_value(b.city_name_full) over (partition by b.grass_date,b.partner_id order by order_id asc) as first_city_name_full
    ,hour(r.created_timestamp) as created_hour

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level b 
left join driver_ops_raw_order_tab r on b.order_id = r.id and b.ref_order_category = r.order_type
where 1=1
and b.source in ('Food','Market')
and b.grass_date between date '2024-07-01' and date '2024-07-31'
and coalesce(b.city_name_full,'na') not in ('na','TestCity','Dien Bien')
)
,driver_cost_raw as
(select 
    case 
        when first_city_name_full in ('HCM City','Ha Noi City' ,'Da Nang City') then first_city_name_full
        when first_city_name_full IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then 'T2'
        else 'T3' end as city_group
    ,(CASE 
        -- WHEN is_nan(bfo.bonus) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub
        ELSE null end)*1.000000   as bonus_usd_all

        , (driver_cost_base + bfo.return_fee_share_basic)*1.000000   as total_driver_cost_base_all
        , (driver_cost_surge + bfo.return_fee_share_surge)*1.000000   as total_driver_cost_surge_all
        ,(CASE 
        -- WHEN is_nan(bfo.bonus_v2) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub_v2
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub_v2
        ELSE null end)*1.000000   as bonus_usd_all_v2
        , (driver_cost_base_v2 + bfo.return_fee_share_basic)*1.000000   as total_driver_cost_base_all_v2
        , (driver_cost_surge_v2 + bfo.return_fee_share_surge)*1.000000   as total_driver_cost_surge_all_v2
            -- ,delivered_by
        ,case 
             
            when city_name_full in ('HCM City', 'Ha Noi City') then 
            (case
            when delivered_by = 'hub' and (new_driver_tier_v2 is null or new_driver_tier_v2 = 'HUB_OTH') then 'HUB03'
            when delivered_by = 'hub' and new_driver_tier_v2 in ('HUB10','HUB08') then 'HUB8|HUB10'
            when delivered_by = 'hub' and new_driver_tier_v2 is not null then new_driver_tier_v2
            when delivered_by != 'hub' and lower(new_driver_tier_v2) like '%hub%' then 'T1' --- outshift
            when delivered_by != 'hub' and new_driver_tier_v2 is null then 'T1'
            when delivered_by != 'hub' and new_driver_tier_v2 = 'part_time' then 'T1'
            else new_driver_tier_v2 end) 
            when city_name_full not in ('HCM City', 'Ha Noi City') then  'part_time' else null end
            driver_tier
        ,*
        ,if(distance<=3.6,1,0) as is_short_distance
        ,(case when bfo.delivered_by = 'hub' then bfo.total_bad_weather_cost_hub else bfo.total_bad_weather_cost_non_hub end) as dr_cost_bw_fee
        ,(case when bfo.delivered_by = 'hub' then bfo.total_late_night_fee_temp_hub else bfo.total_late_night_fee_temp_non_hub end) as dr_cost_late_night
        ,(case when bfo.delivered_by = 'hub' then bfo.total_holiday_fee_temp_hub else bfo.total_holiday_fee_temp_non_hub end) as dr_cost_holiday_fee
        ,CASE
        WHEN created_hour < 11 THEN 'early morning'
        WHEN created_hour < 14 THEN 'lunch'
        WHEN created_hour < 18 THEN 'off peak'
        WHEN created_hour < 21 THEN 'dinner'
        WHEN created_hour >= 21 THEN 'late night' END AS hour_range,
        case 
        when pick_district_id in (1,5,6,7,12,15,17,16,25,22,20,24,21) then 'main'
        else 'rural' end as district_type
from bill_fee_order_base bfo


)
select 
        hour_range as metrics,
        case 
        when delivered_by = 'hub' then driver_tier
        else 'non hub' end as "tier",
        count(distinct order_id)*1.00/count(distinct grass_date) as cnt_order

from driver_cost_raw

where city_name in ('HCM','HN')
GROUP BY 1,2

UNION ALL 

select 
        district_type as metrics,
        case 
        when delivered_by = 'hub' then driver_tier
        else 'non hub' end as "tier",
        count(distinct order_id)*1.00/count(distinct grass_date) as cnt_order

from driver_cost_raw

where city_name in ('HCM','HN')
GROUP BY 1,2

