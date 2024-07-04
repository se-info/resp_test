SELECT "metrics" AS "metrics",
       "period" AS "period",
       "driver_group" AS "driver_group",
       sum(value) AS "sum__value"
FROM
  (WITH report_date AS
     (SELECT DATE(report_date) AS report_date
      FROM (
              (SELECT sequence(current_date - interval '90' day, current_date - interval '1' day) bar)
            CROSS JOIN unnest (bar) as t(report_date))),
        period AS
     (SELECT report_date ,
             'Weekly' AS period_group ,
             'W' || CAST(WEEK(report_date) AS VARCHAR) AS period ,
             DATE_FORMAT(DATE_TRUNC('week', report_date), '%d-%b') AS explain_date --     , DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', report_date) DESC) AS no
 ,
             CAST(7 AS DOUBLE) AS days
      FROM report_date
      WHERE report_date BETWEEN DATE_TRUNC('week', current_date) - interval '84' day AND DATE_TRUNC('week', current_date) - interval '1' day
        UNION ALL
        SELECT report_date ,
               'Daily' AS period_group ,
               DATE_FORMAT(report_date, '%Y-%m-%d') AS period ,
               DATE_FORMAT(report_date, '%a') AS explain_date --     , DENSE_RANK() OVER (ORDER BY report_date DESC) AS no
 ,
               CAST(1 AS DOUBLE) AS days
        FROM report_date WHERE report_date BETWEEN current_date - interval '90' day AND current_date - interval '1' day ),
        kpi AS
     (SELECT created_date AS date_ ,
             '1. Overall District' AS driver_group ,
             pick_hub_location AS hub_location ,
             pick_hub_name AS hub_name ,
             SUM(IF(is_order_picked_at_hub = 1, total_order_delivered, 0)) AS delivered_orders --01. ADO
 ,
             SUM(IF(is_order_picked_at_hub = 1, total_order, 0)) AS total_orders ,
             SUM(IF(is_order_picked_at_hub = 1, total_order_cancelled, 0)) AS cancel_orders --02. Cancel (/ total_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins','2. Late 10-20 mins','3. Late 20+ mins'), total_order_delivered, 0)) AS late_total --03. Late Total ( / delivered_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins'), total_order_delivered, 0)) AS late_0_10_min --04. Late 0 - 10 min ( / delivered_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and range_lt_from_promise_to_actual_delivered in ('2. Late 10-20 mins'), total_order_delivered, 0)) AS late_10_20_min --05. Late 10 - 20 min ( / delivered_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and range_lt_from_promise_to_actual_delivered in ('3. Late 20+ mins'), total_order_delivered, 0)) AS late_over_20_min --06. Late 20+ min ( / delivered_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and source = 'NowFood'
                    and is_asap = 1, total_order_delivered, 0)) AS delivered_nowfood_asap_orders ,
             SUM(IF(is_order_picked_at_hub = 1
                    and source = 'NowFood'
                    and is_asap = 1, total_lt_incharge, 0)) AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1, total_shipping_fee, 0)) AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and source = 'NowFood', total_order_delivered, 0)) AS delivered_nowfood_orders ,
             SUM(IF(is_order_picked_at_hub = 1
                    and source = 'NowFood', total_shipper_rating, 0)) AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
             SUM(IF(is_order_picked_at_hub = 1
                    and source = 'NowFood'
                    and is_asap = 1, total_lt_completion_original, 0)) AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM vnfdbi_opsndrivers.snp_foody_order_performance_tab
      WHERE pick_hub_name IS NOT NULL
        AND created_date BETWEEN current_date - interval '90' day AND current_date - interval '1' day
      GROUP BY 1,
               2,
               3,
               4
      UNION ALL SELECT created_date AS date_ ,
                       '2. Qualified Hub - Del by Hub Driver' AS driver_group ,
                       pick_hub_location AS hub_location ,
                       pick_hub_name AS hub_name ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS delivered_orders --01. ADO
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and is_order_in_hub_shift = 1, total_order, 0)) AS total_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and is_order_in_hub_shift = 1, total_order_cancelled, 0)) AS cancel_orders --02. Cancel (/ total_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins','2. Late 10-20 mins','3. Late 20+ mins')
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS late_total --03. Late Total (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins')
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('2. Late 10-20 mins')
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('3. Late 20+ mins')
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS delivered_nowfood_asap_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and is_order_in_hub_shift = 1, total_lt_incharge, 0)) AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and is_order_in_hub_shift = 1, total_shipping_fee, 0)) AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and source = 'NowFood'
                              and is_order_in_hub_shift = 1, total_order_delivered, 0)) AS delivered_nowfood_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and source = 'NowFood'
                              and is_order_in_hub_shift = 1, total_shipper_rating, 0)) AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 1
                              and is_order_picked_at_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and is_order_in_hub_shift = 1, total_lt_completion_original, 0)) AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM vnfdbi_opsndrivers.snp_foody_order_performance_tab
      WHERE hub_name IS NOT NULL
        AND created_date BETWEEN current_date - interval '90' day AND current_date - interval '1' day
      GROUP BY 1,
               2,
               3,
               4
      UNION ALL SELECT created_date AS date_ ,
                       '3. Qualified Hub - Del by Non Hub Driver' AS driver_group ,
                       pick_hub_location AS hub_location ,
                       pick_hub_name AS hub_name ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS delivered_orders --01. ADO
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and (created_hour between 7 and 21), total_order, 0)) AS total_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and (created_hour between 7 and 21), total_order_cancelled, 0)) AS cancel_orders --02. Cancel (/ total_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins','2. Late 10-20 mins','3. Late 20+ mins')
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS late_total --03. Late Total (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('1. Late 0-10 mins')
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('2. Late 10-20 mins')
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and range_lt_from_promise_to_actual_delivered in ('3. Late 20+ mins')
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS delivered_nowfood_asap_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and (created_hour between 7 and 21), total_lt_incharge, 0)) AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and (created_hour between 7 and 21), total_shipping_fee, 0)) AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and source = 'NowFood'
                              and (created_hour between 7 and 21), total_order_delivered, 0)) AS delivered_nowfood_orders ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and source = 'NowFood'
                              and (created_hour between 7 and 21), total_shipper_rating, 0)) AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
                       SUM(IF(is_order_delivered_by_driver_hub = 0
                              and is_order_qualified_hub = 1
                              and source = 'NowFood'
                              and is_asap = 1
                              and (created_hour between 7 and 21), total_lt_completion_original, 0)) AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM vnfdbi_opsndrivers.snp_foody_order_performance_tab
      WHERE pick_hub_name IS NOT NULL
        AND created_date BETWEEN current_date - interval '90' day AND current_date - interval '1' day
      GROUP BY 1,
               2,
               3,
               4 ),
        union_kpi AS
     (-- All VN
SELECT p.period_group ,
       p.period || ' : ' || p.explain_date AS period ,
       p.days ,
       k.driver_group ,
       '1. VN' AS location ,
       SUM(k.delivered_orders) / p.days AS delivered_ado --01. ADO
 ,
       SUM(k.total_orders) / p.days AS total_ado ,
       SUM(k.cancel_orders) / p.days AS cancel_ado --02. Cancel (/ total_orders)
 ,
       SUM(k.late_total) / p.days AS late_total --03. Late Total (/ delivered_orders)
 ,
       SUM(k.late_0_10_min) / p.days AS late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
       SUM(k.late_10_20_min) / p.days AS late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
       SUM(k.late_over_20_min) / p.days AS late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_asap_orders) / p.days AS delivered_nowfood_asap_ado ,
       SUM(k.assignment_time) / p.days AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
       SUM(k.user_shipping_fee) / p.days AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_orders) / p.days AS delivered_nowfood_ado ,
       SUM(k.avg_rating) / p.days AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
       SUM(k.completion_time) / p.days AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM kpi k
      INNER JOIN period p ON k.date_ = p.report_date
      GROUP BY 1,
               2,
               3,
               4,
               5
      UNION ALL -- Hub location
SELECT p.period_group ,
       p.period || ' : ' || p.explain_date AS period ,
       p.days ,
       k.driver_group ,
       CASE
           WHEN k.hub_location = 'HCM' THEN '2. HCM'
           WHEN k.hub_location = 'HN' THEN '3. HN'
           ELSE '4. OTH'
       END AS location ,
       SUM(k.delivered_orders) / p.days AS delivered_ado --01. ADO
 ,
       SUM(k.total_orders) / p.days AS total_ado ,
       SUM(k.cancel_orders) / p.days AS cancel_ado --02. Cancel (/ total_orders)
 ,
       SUM(k.late_total) / p.days AS late_total --03. Late Total (/ delivered_orders)
 ,
       SUM(k.late_0_10_min) / p.days AS late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
       SUM(k.late_10_20_min) / p.days AS late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
       SUM(k.late_over_20_min) / p.days AS late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_asap_orders) / p.days AS delivered_nowfood_asap_ado ,
       SUM(k.assignment_time) / p.days AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
       SUM(k.user_shipping_fee) / p.days AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_orders) / p.days AS delivered_nowfood_ado ,
       SUM(k.avg_rating) / p.days AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
       SUM(k.completion_time) / p.days AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM kpi k
      INNER JOIN period p ON k.date_ = p.report_date
      GROUP BY 1,
               2,
               3,
               4,
               5
      UNION ALL -- Hub name
SELECT p.period_group ,
       p.period || ' : ' || p.explain_date AS period ,
       p.days ,
       k.driver_group ,
       CASE
           WHEN k.hub_location = 'HCM' THEN '5. HCM - ' || k.hub_name
           WHEN k.hub_location = 'HN' THEN '6. HN - ' || k.hub_name
           ELSE '7. OTH - ' || k.hub_name
       END AS location ,
       SUM(k.delivered_orders) / p.days AS delivered_ado --01. ADO
 ,
       SUM(k.total_orders) / p.days AS total_ado ,
       SUM(k.cancel_orders) / p.days AS cancel_ado --02. Cancel (/ total_orders)
 ,
       SUM(k.late_total) / p.days AS late_total --03. Late Total (/ delivered_orders)
 ,
       SUM(k.late_0_10_min) / p.days AS late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
       SUM(k.late_10_20_min) / p.days AS late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
       SUM(k.late_over_20_min) / p.days AS late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_asap_orders) / p.days AS delivered_nowfood_asap_ado ,
       SUM(k.assignment_time) / p.days AS assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
       SUM(k.user_shipping_fee) / p.days AS user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
       SUM(k.delivered_nowfood_orders) / p.days AS delivered_nowfood_ado ,
       SUM(k.avg_rating) / p.days AS avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
       SUM(k.completion_time) / p.days AS completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
FROM kpi k
      INNER JOIN period p ON k.date_ = p.report_date
      GROUP BY 1,
               2,
               3,
               4,
               5) ,
        final_kpi AS
     (SELECT period_group ,
             period ,
             driver_group ,
             location ,
             delivered_ado --01. ADO
 ,
             total_ado ,
             cancel_ado --02. Cancel (/ total_orders)
 ,
             TRY(cancel_ado / total_ado) AS cancel_rate ,
             late_total --03. Late Total (/ delivered_orders)
 ,
             TRY(late_total / delivered_ado) AS late_rate ,
             late_0_10_min --04. Late 0 - 10 min (/ delivered_orders)
 ,
             TRY(late_0_10_min / delivered_ado) AS late_0_10_min_rate ,
             late_10_20_min --05. Late 10 - 20 min (/ delivered_orders)
 ,
             TRY(late_10_20_min / delivered_ado) AS late_10_20_min_rate ,
             late_over_20_min --06. Late 20+ min (/ delivered_orders)
 ,
             TRY(late_over_20_min / delivered_ado) AS late_over_20_min_rate ,
             delivered_nowfood_asap_ado ,
             assignment_time --07. Assignment Time (/ delivered_nowfood_asap_orders)
 ,
             TRY(assignment_time / delivered_nowfood_asap_ado) AS assignment_time_nowfood_asap ,
             user_shipping_fee --08. User shipping fee (/ delivered_orders)
 ,
             TRY(user_shipping_fee / delivered_ado) AS user_shipping_fee_per_order ,
             delivered_nowfood_ado ,
             avg_rating --09. Avg rating (/ delivered_nowfood_orders)
 ,
             TRY(avg_rating / delivered_nowfood_ado) AS avg_rating_nowfood ,
             completion_time --10. Completion Time (/ delivered_nowfood_asap_orders)
 ,
             TRY(completion_time / delivered_nowfood_asap_ado) AS completion_time_nowfood_asap
      FROM union_kpi ) SELECT f.period_group ,
                              f.period ,
                              f.driver_group ,
                              f.location ,
                              a.metrics ,
                              a.value
   FROM final_kpi f
   CROSS JOIN UNNEST ( ARRAY['01. ADO',
                             '02. Cancel',
                             '03. Late Total',
                             '04. Late 0 - 10 min',
                             '05. Late 10 - 20 min',
                             '06. Late 20+ min',
                             '07. Assignment Time',
                             '08. User shipping fee',
                             '09. Avg rating',
                             '10. Completion Time'], ARRAY[delivered_ado,
                                                           cancel_rate,
                                                           late_rate,
                                                           late_0_10_min_rate,
                                                           late_10_20_min_rate,
                                                           late_over_20_min_rate,
                                                           assignment_time_nowfood_asap,
                                                           user_shipping_fee_per_order,
                                                           avg_rating_nowfood,
                                                           completion_time_nowfood_asap] ) a (metrics, value)
   WHERE is_nan(a.value) = FALSE) AS "expr_qry"
WHERE (period_group = 'Daily')
GROUP BY "metrics",
         "period",
         "driver_group"
ORDER BY "sum__value" DESC
LIMIT 2000000