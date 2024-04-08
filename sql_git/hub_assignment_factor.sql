with 
base_tab as (
    select  
        ref_order_id
        , created_hour
        , city_id
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev
    where 1=1 
    and source = 'NowFood' -- NowFood & NowShip
    and city_id in (217, 218, 220)
    and created_year_month = '${created_year_month}' --e.g. 2023-Feb
    and is_order_qualified_hub = 1
    and created_hour >= 6 and created_hour <= 22
)
, hour_hub_qualified_order_tab as (
    select 
        city_id
        , created_hour
        , count(ref_order_id) as cnt
    from base_tab
    group by city_id, created_hour
)
, daily_hub_qualified_order_tab as (
    select 
        city_id
        , count(ref_order_id) as cnt
    from base_tab
    group by city_id
)
, hour_hub_qualified_rate_tab as (
    select 
        t0.city_id
        , ARRAY [1.0000 * t6.cnt / t0.cnt] 
            || 1.0000 * t7.cnt / t0.cnt
            || 1.0000 * t8.cnt / t0.cnt
            || 1.0000 * t9.cnt / t0.cnt
            || 1.0000 * t10.cnt / t0.cnt
            || 1.0000 * t11.cnt / t0.cnt
            || 1.0000 * t12.cnt / t0.cnt
            || 1.0000 * t13.cnt / t0.cnt
            || 1.0000 * t14.cnt / t0.cnt
            || 1.0000 * t15.cnt / t0.cnt
            || 1.0000 * t16.cnt / t0.cnt
            || 1.0000 * t17.cnt / t0.cnt
            || 1.0000 * t18.cnt / t0.cnt
            || 1.0000 * t19.cnt / t0.cnt
            || 1.0000 * t20.cnt / t0.cnt
            || 1.0000 * t21.cnt / t0.cnt
            || 1.0000 * t22.cnt / t0.cnt as distribution
    from daily_hub_qualified_order_tab t0
    left join (select * from hour_hub_qualified_order_tab where created_hour = 6) t6 on t0.city_id = t6.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 7) t7 on t0.city_id = t7.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 8) t8 on t0.city_id = t8.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 9) t9 on t0.city_id = t9.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 10) t10 on t0.city_id = t10.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 11) t11 on t0.city_id = t11.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 12) t12 on t0.city_id = t12.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 13) t13 on t0.city_id = t13.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 14) t14 on t0.city_id = t14.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 15) t15 on t0.city_id = t15.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 16) t16 on t0.city_id = t16.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 17) t17 on t0.city_id = t17.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 18) t18 on t0.city_id = t18.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 19) t19 on t0.city_id = t19.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 20) t20 on t0.city_id = t20.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 21) t21 on t0.city_id = t21.city_id
    left join (select * from hour_hub_qualified_order_tab where created_hour = 22) t22 on t0.city_id = t22.city_id
)

select * from hour_hub_qualified_rate_tab order by city_id