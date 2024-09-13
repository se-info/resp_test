with bill_fee_order_base as
(select 
    *
    ,first_value(city_name_full) over (partition by grass_date,partner_id order by order_id asc) as first_city_name_full
    

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
where 1=1
and source not in ('Food','Market')
and grass_date between date '2024-07-01' and date '2024-07-31'
and coalesce(city_name_full,'na') not in ('na','TestCity','Dien Bien')
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
             
            when first_city_name_full in ('HCM City', 'Ha Noi City') then 
            (case
            when delivered_by = 'hub' and (new_driver_tier_v2 is null or new_driver_tier_v2 = 'HUB_OTH') then 'HUB03'
            when delivered_by = 'hub' and new_driver_tier_v2 in ('HUB10','HUB08') then 'HUB8|HUB10'
            when delivered_by = 'hub' and new_driver_tier_v2 is not null then new_driver_tier_v2
            when delivered_by != 'hub' and lower(new_driver_tier_v2) like '%hub%' then 'T1' --- outshift
            when delivered_by != 'hub' and new_driver_tier_v2 is null then 'T1'
            when delivered_by != 'hub' and new_driver_tier_v2 = 'part_time' then 'T1'
            else new_driver_tier_v2 end) 
            when first_city_name_full not in ('HCM City', 'Ha Noi City') then  'part_time' else null end
            driver_tier
        ,*
        ,if(distance<=3.6,1,0) as is_short_distance
        ,(case when bfo.delivered_by = 'hub' then bfo.total_bad_weather_cost_hub else bfo.total_bad_weather_cost_non_hub end) as dr_cost_bw_fee
        ,(case when bfo.delivered_by = 'hub' then bfo.total_late_night_fee_temp_hub else bfo.total_late_night_fee_temp_non_hub end) as dr_cost_late_night
        ,(case when bfo.delivered_by = 'hub' then bfo.total_holiday_fee_temp_hub else bfo.total_holiday_fee_temp_non_hub end) as dr_cost_holiday_fee
from bill_fee_order_base bfo
)
select 
    date_trunc('month',grass_date) as month_
    -- ,exchange_rate
    ,CASE 
        WHEN first_city_name_full IN 
        ('HCM City',
        'Ha Noi City') THEN 'HCM & HN'
        WHEN first_city_name_full IN
        ('Da Nang City',
        'Dong Nai',
        'Can Tho City',
        'Binh Duong',
        'Hai Phong City',
        'Hue City',
        'Vung Tau',
        'Bac Ninh',
        'Khanh Hoa',
        'Nghe An',
        'Thai Nguyen',
        'Quang Ninh',
        'Lam Dong',
        'Quang Nam') THEN 'DN & OTH'  
        ELSE 'new_cities' END AS cities
    -- ,city_group
    ,1 as "1"
    ,driver_tier
    -- ,sum(bonus_usd_all + total_driver_cost_base_all + total_driver_cost_surge_all) as driver_cost_v1
    -- ,sum(bonus_usd_all_v2 + total_driver_cost_base_all_v2 + total_driver_cost_surge_all_v2) as driver_cost_v2
    ,sum(bonus_usd_all + total_driver_cost_base_all + total_driver_cost_surge_all)/count(distinct order_id) as cpo_v1
    ,sum(bonus_usd_all_v2 + total_driver_cost_base_all_v2 + total_driver_cost_surge_all_v2)/count(distinct order_id) as cpo_v2
    ,sum(dr_cost_bw_fee)/count(distinct order_id) AS bwf 
    ,sum(dr_cost_late_night)/count(distinct order_id) AS late_night 
    ,sum(dr_cost_holiday_fee)/count(distinct order_id) AS holiday_fee
    ,count(distinct order_id) as ado
    ,count(distinct (partner_id,grass_date))*1.00/count(distinct grass_date) as unique_a1
    ,count(distinct case when delivered_by = 'hub' then order_id else null end)*1.0000/count(distinct order_id) as pp_hub
    ,count(distinct grass_date) as days
    ,count(distinct case when is_short_distance = 1 then order_id else null end) as short_ado
    ,count(distinct case when is_short_distance = 0 then order_id else null end) as long_ado
    ,count(distinct case when dr_cost_bw_fee > 0 then order_id else null end) as bw_ado
    ,count(distinct case when dr_cost_late_night > 0 then order_id else null end) as ln_ado


from driver_cost_raw
group by 1,2,3,4
