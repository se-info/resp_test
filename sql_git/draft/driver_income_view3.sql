SELECT "period_group" AS "period_group",
       "city_tier" AS "city_tier",
       "city_name" AS "city_name",
       "shipper_tier" AS "shipper_tier",
       "period" AS "period",
       try(sum(total_income)/sum(working_days)) AS "income_per_day"
FROM
  (WITH report_date AS
     (SELECT DATE(report_date) AS report_date
      FROM (
              (SELECT sequence(current_date - interval '35' day, current_date - interval '1' day) bar)
            CROSS JOIN unnest (bar) as t(report_date))),
        period AS
     (SELECT report_date ,
             '1. Weekly' AS period_group ,
             'W' || CAST(WEEK(report_date) AS VARCHAR) AS period ,
             DATE_FORMAT(DATE_TRUNC('week', report_date), '%d-%b') AS explain_date ,
             DENSE_RANK() OVER (
                                ORDER BY DATE_TRUNC('week', report_date) DESC) AS no ,
             CAST(7 AS DOUBLE) AS days
      FROM report_date
      WHERE report_date BETWEEN DATE_TRUNC('week', current_date) - interval '28' day AND DATE_TRUNC('week', current_date) - interval '1' day
        UNION ALL
        SELECT report_date ,
               '2. Daily' AS period_group ,
               DATE_FORMAT(report_date, '%Y-%m-%d') AS period ,
               DATE_FORMAT(report_date, '%a') AS explain_date ,
               DENSE_RANK() OVER (
                                  ORDER BY report_date DESC) AS no ,
               CAST(1 AS DOUBLE) AS days
        FROM report_date WHERE report_date BETWEEN current_date - interval '30' day AND current_date - interval '1' day ),
        all_drivers AS
     (SELECT d.report_date ,
             d.shipper_id,
             d.city_name,
             CASE
                 WHEN d.city_name IN ('HCM City',
                                      'Ha Noi City',
                                      'Da Nang City') THEN 'T1'
                 WHEN d.city_name IN ('Hai Phong City',
                                      'Hue City',
                                      'Can Tho City',
                                      'Dong Nai',
                                      'Binh Duong',
                                      'Vung Tau') THEN 'T2'
                 ELSE 'T3'
             END AS city_tier ,
             d.current_driver_tier AS shipper_tier,
             d.total_online_time AS online_time,
             IF(d.cnt_total_order_delivered > 0, 1, 0) AS working_days ,
             DATE(FROM_UNIXTIME(si.create_time - 3600)) AS onboard_date ,
             IF(d.current_driver_tier = 'Hub', COALESCE(h.in_shift_work_time, 0), NULL) AS inshift_online_time ,
             IF(d.current_driver_tier = 'Hub', COALESCE(d.total_online_time, 0) - COALESCE(h.in_shift_work_time, 0), NULL) AS outshift_online_time ,
             COALESCE(i.total_income, 0) AS total_income ,
             COALESCE(i.nonhub_income, 0) AS nonhub_income ,
             COALESCE(i.hub_income, 0) AS hub_income
      FROM vnfdbi_opsndrivers.snp_foody_shipper_daily_report d
      LEFT JOIN
        (SELECT partner_id AS shipper_id ,
                date_ AS report_date ,
                SUM(total_earning_before_tax) AS total_income ,
                SUM(total_earning_non_hub) AS nonhub_income ,
                SUM(total_earning_hub) AS hub_income
         FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab
         WHERE date_ BETWEEN current_date - interval '35' day AND current_date - interval '1' day
         GROUP BY 1,
                  2) i ON d.shipper_id = i.shipper_id
      AND d.report_date = i.report_date
      LEFT JOIN vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab h ON d.shipper_id = h.shipper_id
      AND d.report_date = h.report_date
      AND d.current_driver_tier = 'Hub'
      LEFT JOIN shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si ON d.shipper_id = si.uid
      WHERE d.report_date BETWEEN current_date - interval '35' day AND current_date - interval '1' day
        AND d.current_driver_tier != 'full-time' ),
        segment AS
     (SELECT report_date ,
             shipper_id,
             city_name,
             city_tier ,
             shipper_tier ,
             IF(DATE_DIFF('day', onboard_date, report_date) + 1 > 14, 'Existing', 'New') AS shipper_type ,
             online_time,
             working_days ,
             total_income
      FROM all_drivers) SELECT s.city_tier ,
                               s.city_name ,
                               s.shipper_tier ,
                               s.shipper_type ,
                               p.period_group ,
                               p.period || ' : ' || p.explain_date AS period ,
                               SUM(working_days) AS working_days ,
                               SUM(online_time) AS online_time ,
                               SUM(total_income) AS total_income
   FROM period p
   LEFT JOIN segment s ON p.report_date = s.report_date
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6
   HAVING SUM(online_time) > 0
   UNION ALL SELECT s.city_tier ,
                    'All*' AS city_name ,
                    s.shipper_tier ,
                    s.shipper_type ,
                    p.period_group ,
                    p.period || ' : ' || p.explain_date AS period ,
                    SUM(working_days) AS working_days ,
                    SUM(online_time) AS online_time ,
                    SUM(total_income) AS total_income
   FROM period p
   LEFT JOIN segment s ON p.report_date = s.report_date
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6
   HAVING SUM(online_time) > 0) AS "expr_qry"
WHERE (lower(city_name) not like '%testcity%')
GROUP BY "period_group",
         "city_tier",
         "city_name",
         "shipper_tier",
         "period"
ORDER BY "income_per_day" DESC
LIMIT 2000000