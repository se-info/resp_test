WITH district(city_group, district_name, district_rename) AS (
VALUES
('HCM', 'Binh Chanh', '20. Bình Chánh')
, ('HCM', 'Binh Tan', '06. Bình Tân')
, ('HCM', 'Binh Thanh', '01. Bình Thạnh')
, ('HCM', 'District 1', '08. Quận 1')
, ('HCM', 'District 10', '09. Quận 10')
, ('HCM', 'District 11', '18. Quận 11')
, ('HCM', 'District 12', '19. Quận 12')
, ('HCM', 'District 2', '14. Quận 2')
, ('HCM', 'District 3', '10. Quận 3')
, ('HCM', 'District 4', '15. Quận 4')
, ('HCM', 'District 5', '16. Quận 5')
, ('HCM', 'District 6', '13. Quận 6')
, ('HCM', 'District 7', '04. Quận 7')
, ('HCM', 'District 8', '12. Quận 8')
, ('HCM', 'District 9', '17. Quận 9')
, ('HCM', 'Go Vap', '03. Gò Vấp')
, ('HCM', 'Hoc Mon', '22. Hóc Môn')
, ('HCM', 'Nha Be', '21. Nhà Bè')
, ('HCM', 'Cu Chi', '23. Củ Chi')
, ('HCM', 'Can Gio', '24. Can Gio')
, ('HCM', 'Others', '25. Khác')
, ('HCM', 'Phu Nhuan', '07. Phú Nhuận')
, ('HCM', 'Tan Binh', '02. Tân Bình')
, ('HCM', 'Tan Phu', '05. Tân Phú')
, ('HCM', 'Thu Duc', '11. Thủ Đức')
, ('HN', 'Ba Dinh', '05. Ba Dinh')
, ('HN', 'Bac Tu Liem', '10. Bac Tu Liem')
, ('HN', 'Cau Giay', '01. Cau Giay')
, ('HN', 'Chuong My', '17. Chuong My')
, ('HN', 'Dan Phuong', '19. Dan Phuong')
, ('HN', 'Dong Da', '02. Dong Da')
, ('HN', 'Gia Lam', '14. Gia Lam')
, ('HN', 'Ha Dong', '06. Ha Dong')
, ('HN', 'Hai Ba Trung', '04. Hai Ba Trung')
, ('HN', 'Hoai Duc', '15. Hoai Duc')
, ('HN', 'Hoan Kiem', '09. Hoan Kiem')
, ('HN', 'Hoang Mai', '07. Hoang Mai')
, ('HN', 'Long Bien', '12. Long Bien')
, ('HN', 'Nam Tu Liem', '08. Nam Tu Liem')
, ('HN', 'Tay Ho', '13. Tay Ho')
, ('HN', 'Thanh Oai', '18. Thanh Oai')
, ('HN', 'Thanh Tri', '11. Thanh Tri')
, ('HN', 'Thanh Xuan', '03. Thanh Xuan')
, ('HN', 'Dong Anh', '16. Dong Anh')
, ('HN', 'Thuong Tin', '20. Thuong Tin')
, ('HN', 'Soc Son', '21. Soc Son')
, ('HN', 'Phu Xuyen', '22. Phu Xuyen')
, ('HN', 'Thach That', '23. Thach That')
, ('HN', 'Ba Vi', '24. Ba Vi')
, ('HN', 'Others', '25. Khác')
)

, data AS
(SELECT
    created_date,
    hour(created_timestamp) as created_hour,
    '2. NowShip' as service,
    ns.city_group,
    COALESCE(d.district_rename, '25. Others') AS district_name,
    ns.city_group || '-' || district_rename as district,
    
count (distinct uid) * 1.000 as gross_order,
    count (distinct case when order_status = 'Delivered' then uid else null end) * 1.000 as net_order,
    count (distinct case when order_status not in ('Delivered','Returned') then uid else null end) * 1.000 as canceled_order,
    count (distinct case when is_no_driver_assign = 1 then uid else null end) * 1.000 as cancel_no_driver
FROM
    foody_bi_anlys.snp_foody_nowship_performance_tab ns
LEFT JOIN
    district d ON COALESCE(ns.district_name, 'Others') = d.district_name AND ns.city_group = d.city_group
WHERE
    created_date between current_date - interval '30' day and current_date - interval '1' day
AND ns.city_group in ('HCM', 'HN')
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
    created_date,
    hour(created_timestamp) as created_hour,
    '2.1. NowShip OnShopee' as service,
    ns.city_group,
    COALESCE(d.district_rename, '25. Others') AS district_name,
    ns.city_group || '-' || district_rename as district,

count (distinct uid) * 1.000 as gross_order,
    count (distinct case when order_status = 'Delivered' then uid else null end) * 1.000 as net_order,
    count (distinct case when order_status not in ('Delivered','Returned') then uid else null end) * 1.000 as canceled_order,
    count (distinct case when is_no_driver_assign = 1 then uid else null end) * 1.000 as cancel_no_driver
FROM
    foody_bi_anlys.snp_foody_nowship_performance_tab ns
LEFT JOIN
    district d ON COALESCE(ns.district_name, 'Others') = d.district_name AND ns.city_group = d.city_group
WHERE
    created_date between current_date - interval '30' day and current_date - interval '1' day
AND ns.city_group in ('HCM', 'HN')
AND source = 'now_ship_shopee'
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
    created_date,
    hour(created_timestamp) as created_hour,
    '2.2. NowShip OffShopee' as service,
    ns.city_group,
    COALESCE(d.district_rename, '25. Others') AS district_name,
    ns.city_group || '-' || district_rename as district,

count (distinct uid) * 1.000 as gross_order,
    count (distinct case when order_status = 'Delivered' then uid else null end) * 1.000 as net_order,
    count (distinct case when order_status not in ('Delivered','Returned') then uid else null end) * 1.000 as canceled_order,
    count (distinct case when is_no_driver_assign = 1 then uid else null end) * 1.000 as cancel_no_driver
FROM
    foody_bi_anlys.snp_foody_nowship_performance_tab ns
LEFT JOIN
    district d ON COALESCE(ns.district_name, 'Others') = d.district_name AND ns.city_group = d.city_group
WHERE
    created_date between current_date - interval '30' day and current_date - interval '1' day
AND ns.city_group in ('HCM', 'HN')
AND source != 'now_ship_shopee'
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
    created_date,
    inflow_hour as created_hour,
    IF(foody_service = 'Food', '4. Food', '3. Fresh+Market') AS service,
    ns.city_group,
    COALESCE(d.district_rename, '25. Others') AS district_name,
    ns.city_group || '-' || district_rename as district,
    sum (cnt_total_order) * 1.000 as gross_order,
    sum(case when is_canceled = 0 then cnt_total_order END) * 1.000 as net_order,
    sum(case when is_canceled = 1 then cnt_total_order END) * 1.000 as canceled_order,
    sum(case when is_canceled = 1 and cancel_reason = 'No driver' then cnt_total_order END) * 1.000 as cancel_no_driver
FROM
    foody_bi_anlys.snp_foody_order_cancellation_db ns
LEFT JOIN
    district d ON COALESCE(ns.district_name, 'Others') = d.district_name AND ns.city_group = d.city_group
WHERE
    inflow_date between current_date - interval '30' day and current_date - interval '1' day
AND ns.city_group in ('HCM', 'HN')
GROUP BY 1,2,3,4,5,6
    )

, serrvice_union as
(SELECT
    *
FROM
    data

UNION ALL

SELECT
    created_date,
    created_hour,
    '1. Total' as service,
    city_group,
    district_name,
    district,
    SUM(gross_order) AS gross_order,
    SUM(net_order) AS net_order,
    SUM(canceled_order) AS canceled_order,
    SUM(cancel_no_driver) AS cancel_no_driver
FROM
    data
WHERE
    service NOT IN ('2.1. NowShip OnShopee', '2.2. NowShip OffShopee')
GROUP BY
    1,2,3,4,5,6
    )
select created_date, created_hour,service,city_group,district_name
        ,coalesce(gross_order,0) as gross_order
        ,coalesce(net_order,0) as net_order
        ,coalesce(canceled_order,0) as canceled_order
        ,coalesce(cancel_no_driver,0) as cancel_no_driver

from 
(SELECT
    created_date,
    IF(created_hour between 0 and 9, '0' || CAST(created_hour AS VARCHAR), CAST(created_hour AS VARCHAR)) AS created_hour,
    service,
    city_group,
    district_name,
    district,
    gross_order,
    net_order,
    canceled_order,
    cancel_no_driver
FROM
    serrvice_union

UNION ALL

SELECT
    created_date,
    '*Total' created_hour,
    service,
    city_group,
    district_name,
    district,
    SUM(gross_order) AS gross_order,
    SUM(net_order) AS net_order,
    SUM(canceled_order) AS canceled_order,
    SUM(cancel_no_driver) AS cancel_no_driver
FROM
    serrvice_union
GROUP BY
    1,2,3,4,5,6)
where created_date >= date((current_date) - interval '15' day)
and created_date < date(current_date)