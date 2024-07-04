WITH report_date AS
(SELECT
    DATE(report_date) AS report_date
FROM
    ((SELECT sequence(current_date - interval '35' day, current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(report_date)
))
, period AS
(SELECT
    report_date
    , 'Weekly' AS period_group
    , 'W' || CAST(WEEK(report_date) AS VARCHAR) AS period
    , DATE_FORMAT(DATE_TRUNC('week', report_date) + interval '4' day, '%d-%b') AS explain_date
    , DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', report_date) DESC) AS no
    , 7.000000 AS days
FROM
    report_date
WHERE
    report_date BETWEEN  DATE_TRUNC('week', current_date) - interval '28' day AND  DATE_TRUNC('week', current_date) - interval '1' day

UNION ALL

SELECT
    report_date
    , 'Daily' AS period_group
    , DATE_FORMAT(report_date, '%Y-%m-%d') AS period
    , DATE_FORMAT(report_date, '%a') AS explain_date
    , DENSE_RANK() OVER (ORDER BY report_date DESC) AS no
    , 1.000000 AS days
FROM
    report_date
    )
    
, all_metrics AS 
(
-- NowFood 
(SELECT
    IF(city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City', 'Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Vung Tau', 'Binh Duong'), city_name, 'OTH') AS city_group
    , city_name
    , '2. NowFood' AS source
    , CASE
        WHEN foody_service = 'Food' THEN '2.1. Food'
        WHEN foody_service LIKE '%- Fresh' THEN '2.2. Fresh'
        WHEN foody_service LIKE '%- Non Fresh' THEN '2.3. Market'
    ELSE foody_service END AS food_service
    , created_date AS report_date
    , SUM(cnt_total_order) AS submitted_orders
    , SUM(IF(is_del = 1, cnt_total_order, 0)) AS delivered_orders
    , SUM(cnt_total_order) - SUM(IF(is_del = 1, cnt_total_order, 0)) AS cancelled_quit_orders
    , SUM(IF(cancel_reason = 'No driver' AND is_canceled = 1, cnt_total_order, 0)) AS cancelled_no_driver_orders
FROM foody_bi_anlys.snp_foody_order_cancellation_db
WHERE 1=1
AND city_name NOT IN ('Phu Yen','Binh Dinh','Thanh Hoa','Dak Lak','Gia Lai','Ha Tinh')
AND created_date BETWEEN current_date - interval '35' day AND current_date - interval '1' day
GROUP BY 1,2,3,4,5
)

UNION ALL

-- NowShip
(SELECT
    IF(city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City', 'Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Vung Tau', 'Binh Duong'), city_name, 'OTH') AS city_group
    , city_name
    , '3. NowShip' AS source
    , '3.0. NowShip' AS food_service
    , created_date AS report_date
    , COUNT(DISTINCT IF(source = 'now_ship_shopee' and (hour(created_timestamp) >= 18 or hour(created_timestamp) < 6) and created_date >= date'2021-07-26', null, uid)) AS submitted_orders
    , COUNT(DISTINCT CASE WHEN source = 'now_ship_shopee' and (hour(created_timestamp) >= 18 or hour(created_timestamp) < 6) and created_date >= date'2021-07-26' THEN NULL
                                ELSE CASE WHEN order_status = 'Delivered' THEN uid ELSE NULL END END) AS delivered_orders
    , COUNT(DISTINCT CASE WHEN source = 'now_ship_shopee' and (hour(created_timestamp) >= 18 or hour(created_timestamp) < 6) and created_date >= date'2021-07-26' THEN NULL
                                ELSE CASE WHEN order_status != 'Delivered' THEN uid ELSE NULL END END) AS cancelled_quit_orders
    , COUNT(DISTINCT CASE WHEN source = 'now_ship_shopee' and (hour(created_timestamp) >= 18 or hour(created_timestamp) < 6) and created_date >= date'2021-07-26' THEN NULL
                                ELSE CASE WHEN is_no_driver_assign = 1 THEN uid ELSE NULL END END) AS cancelled_no_driver_orders
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab
WHERE 1=1
AND city_name NOT IN ('Phu Yen','Binh Dinh','Thanh Hoa','Dak Lak','Gia Lai','Ha Tinh')
AND created_date BETWEEN current_date - interval '35' day AND current_date - interval '1' day
GROUP BY 1,2,3,4,5
    )
)
, time_metrics AS
(SELECT
    p.period_group
    , p.period || ' : ' || p.explain_date AS period
    , p.no
    , CASE
        WHEN a.city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN 'Tier 1'
        WHEN a.city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Vung Tau', 'Binh Duong') THEN 'Tier 2'
    ELSE 'Tier 3' END AS tier
    , a.city_name AS city_group
    , a.source
    , a.food_service
    , p.days
    , SUM(a.submitted_orders) / p.days AS submitted_ado
    , SUM(a.delivered_orders) / p.days AS delivered_ado
    , SUM(a.cancelled_quit_orders) / p.days AS cancelled_quit_ado
    , SUM(a.cancelled_no_driver_orders) / p.days AS cancelled_no_driver_ado
FROM
    period p
LEFT JOIN
    all_metrics a
ON
    p.report_date = a.report_date
GROUP BY
    1,2,3,4,5,6,7,8
    )
, union_metrics AS
(SELECT
    *
FROM
    time_metrics

UNION ALL

SELECT
    period_group
    , period
    , no
    , tier
    , city_group
    , source
    , '2.0. NowFood' AS food_service
    , days
    , SUM(submitted_ado) AS submitted_ado
    , SUM(delivered_ado) AS delivered_ado
    , SUM(cancelled_quit_ado) AS cancelled_quit_ado
    , SUM(cancelled_no_driver_ado) AS cancelled_no_driver_ado
FROM
    time_metrics
WHERE
    source = '2. NowFood'
GROUP BY
    1,2,3,4,5,6,7,8

UNION ALL

SELECT
    period_group
    , period
    , no
    , tier
    , city_group
    , '1. NowFood & NowShip' AS source
    , '1.0. NowFood & NowShip' AS food_service
    , days
    , SUM(submitted_ado) AS submitted_ado
    , SUM(delivered_ado) AS delivered_ado
    , SUM(cancelled_quit_ado) AS cancelled_quit_ado
    , SUM(cancelled_no_driver_ado) AS cancelled_no_driver_ado
FROM
    time_metrics
GROUP BY
    1,2,3,4,5,6,7,8
    )
, tier_union_metrics AS
(SELECT
    *
FROM
    union_metrics

UNION ALL

SELECT
    period_group
    , period
    , no
    , tier
    , 'All.' AS city_group
    , source
    , food_service
    , days
    , SUM(submitted_ado) AS submitted_ado
    , SUM(delivered_ado) AS delivered_ado
    , SUM(cancelled_quit_ado) AS cancelled_quit_ado
    , SUM(cancelled_no_driver_ado) AS cancelled_no_driver_ado
FROM
    union_metrics
GROUP BY
    1,2,3,4,5,6,7,8
    )
, final_union_metrics AS
(SELECT
    *
FROM
    tier_union_metrics

UNION ALL

SELECT
    period_group
    , period
    , no
    , '' AS tier
    , 'VN' AS city_group
    , source
    , food_service
    , days
    , SUM(submitted_ado) AS submitted_ado
    , SUM(delivered_ado) AS delivered_ado
    , SUM(cancelled_quit_ado) AS cancelled_quit_ado
    , SUM(cancelled_no_driver_ado) AS cancelled_no_driver_ado
FROM
    tier_union_metrics
WHERE 
    city_group != 'All.'
GROUP BY
    1,2,3,4,5,6,7,8
    )
, final_metrics AS
(SELECT
    period_group
    , period
    , no
    , tier
    , city_group
    , source
    , food_service
    , submitted_ado
    , delivered_ado
    , IF(COALESCE(submitted_ado, 0) = 0, NULL, cancelled_quit_ado / submitted_ado) AS cancelled_quit_percent
    , IF(COALESCE(submitted_ado, 0) = 0, NULL, cancelled_no_driver_ado / submitted_ado) AS cancelled_no_driver_percent
FROM
    final_union_metrics
    )
, unpivot AS
(SELECT
    f.period_group
    , f.period
    , f.tier
    , f.city_group
    , f.no
    , f.source
    , f.food_service
    , CASE
        WHEN f.source = '2. NowFood' AND a.metrics = 'c. Cancelled & Quit' THEN 'c. Cancelled & Quit'
        WHEN f.source = '3. NowShip' AND a.metrics = 'c. Cancelled & Quit' THEN 'c. Cancelled & Returned'
        WHEN f.source = '1. NowFood & NowShip' AND a.metrics = 'c. Cancelled & Quit' THEN 'c. Cancelled & Quit/Returned'
    ELSE a.metrics END AS metrics
    , a.value
FROM
    final_metrics f
CROSS JOIN
    UNNEST
        (
        ARRAY['a. Submitted', 'b. Delivered', 'c. Cancelled & Quit', 'd. Cancelled No Driver'],
        ARRAY[submitted_ado, delivered_ado, cancelled_quit_percent, cancelled_no_driver_percent]
        ) a (metrics, value)
    )

SELECT
    period_group
    , period
    , tier
    , city_group
    , source
    , food_service
    , metrics
    , value
FROM
    unpivot

UNION ALL

SELECT
    'WoW' AS period_group
    , 'WoW' AS period
    , w1.tier
    , w1.city_group
    , w1.source
    , w1.food_service
    , w1.metrics
    , IF(COALESCE(w2.value, 0) = 0, NULL, IF(w1.metrics IN ('c. Cancelled & Quit', 'd. Cancelled No Driver') , w1.value - w2.value, w1.value / w2.value - 1)) AS value
FROM
    (SELECT * FROM unpivot WHERE no = 1 AND period_group = 'Weekly') w1
LEFT JOIN
    (SELECT * FROM unpivot WHERE no = 2 AND period_group = 'Weekly') w2
ON
    w1.tier = w2.tier AND w1.city_group = w2.city_group AND w1.source = w2.source AND w1.food_service = w2.food_service AND w1.metrics = w2.metrics
