with raw as 
(select 
        ns.*,
        case 
        when distance <= 3 then '1. 0 - 3'
        when distance <= 5 then '2. 3 - 5'
        when distance <= 10 then '3. 5 - 10'
        when distance <= 15 then '4. 10 - 15'
        when distance <= 20 then '5. 15 - 20'
        when distance > 20 then '6. +20km' end as distance_,
        r.assign_type,
        case 
        when source in ('now_ship_shopee','spx_portal') then 'e2c'
        else 'c2c' end as new_source

FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_spxi_order_details_tab__daily_s0_live ns

left join 
(select 
        order_code,
        case 
        when group_id > 0 and order_assign_type != 'Group' then 'stack'
        when group_id > 0 and order_assign_type = 'Group' then 'group'
        else 'single' end as assign_type
from driver_ops_raw_order_tab
) r on r.order_code = ns.order_code

where grass_date between date'2024-08-01' and date'2024-08-01'
)
select 
        date_trunc('month',grass_date) as "month",
        new_source,
        distance_ as "range",
        assign_type,
        coalesce(city_group,'VN') as city_group,
        count(distinct case when is_del = 1 then order_code else null end) as cnt_net_order,
        TRY(1.0000 * SUM(CASE WHEN is_asap = 1 AND is_valid_lt_incharge = 1 THEN lt_incharge ELSE 0 END) / COUNT(DISTINCT CASE WHEN is_asap = 1 AND is_valid_lt_incharge = 1 THEN uid ELSE NULL END)) AS avg_assign,
        TRY(1.0000 * SUM(CASE WHEN is_valid_lt_pickup = 1 THEN lt_pickup ELSE 0 END) / COUNT(DISTINCT CASE WHEN is_valid_lt_pickup = 1 THEN uid ELSE NULL END)) AS avg_pu,
        TRY(1.0000 * SUM(CASE WHEN is_del = 1 AND is_valid_lt_deliver = 1 THEN lt_deliver ELSE 0 END) / COUNT(DISTINCT CASE WHEN is_del = 1 AND is_valid_lt_deliver = 1 THEN uid ELSE NULL END)) AS avg_deli,
        TRY(1.0000 * SUM(CASE WHEN is_return = 1 AND is_valid_lt_return = 1 THEN lt_return ELSE 0 END) / COUNT(DISTINCT CASE WHEN is_return = 1 AND is_valid_lt_return = 1 THEN uid ELSE NULL END)) AS avg_return,
        TRY(1.0000 * SUM(CASE WHEN is_asap = 1 AND is_del = 1 AND is_valid_lt_e2e = 1 THEN lt_e2e ELSE 0 END) / COUNT(DISTINCT CASE WHEN is_asap = 1 AND is_del = 1 AND is_valid_lt_e2e = 1 THEN uid ELSE NULL END)) AS avg_e2e,
        APPROX_PERCENTILE(CASE WHEN is_asap = 1 AND is_valid_lt_incharge = 1 THEN lt_incharge ELSE NULL END,0.95) AS p95_assign,
        APPROX_PERCENTILE(CASE WHEN is_valid_lt_pickup = 1 THEN lt_pickup ELSE NULL END,0.95) AS p95_pu,
        APPROX_PERCENTILE(CASE WHEN is_del = 1 AND is_valid_lt_deliver = 1 THEN lt_deliver ELSE NULL END,0.95) AS p95_deli,
        APPROX_PERCENTILE(CASE WHEN is_return = 1 AND is_valid_lt_return = 1 THEN lt_return ELSE NULL END,0.95) AS p95_return,
        APPROX_PERCENTILE(CASE WHEN is_asap = 1 AND is_del = 1 AND is_valid_lt_e2e = 1 THEN lt_e2e ELSE NULL END,0.95) AS p95_e2e

from raw 
group by 1,2,3,4,grouping sets (city_group,())