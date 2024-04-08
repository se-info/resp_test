/*
context:
    list driver:
    inactive từ 15-30 ngày kể từ ngày  2022-06-03
    > active_time trong từ date 2022-02-04 > 2022-02-19
    > Type shipper check vào ngày 6/3 là Non-Hub
    > in HN > có đơn phát sinh trước 18h ngày 8-3
    này a run tầm 17h và gửi nhé > data delay thiếu đủ tính qua đợt 2
    gui dev de add point 500
*/

with order_detail AS 
(SELECT
    dot.uid as shipper_id
    ,dot.ref_order_id as order_id
    ,dot.ref_order_code as order_code
    ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
    ,dot.ref_order_category
    ,case when dot.ref_order_category = 0 then 'order_delivery'
        when dot.ref_order_category = 3 then 'now_moto'
        when dot.ref_order_category = 4 then 'now_ship'
        when dot.ref_order_category = 5 then 'now_ship'
        when dot.ref_order_category = 6 then 'now_ship_shopee'
        when dot.ref_order_category = 7 then 'now_ship_sameday'
        else null end source
    ,dot.ref_order_status
    ,dot.order_status
    ,case when dot.order_status = 1 then 'Pending'
        when dot.order_status in (100,101,102) then 'Assigning'
        when dot.order_status in (200,201,202,203,204) then 'Processing'
        when dot.order_status in (300,301) then 'Error'
        when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
        else null end as order_status_group

    ,dot.is_asap

    ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
    ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date

    ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
WHERE 1=1 
AND dot.order_status = 400 -- delivered orders 
AND case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end <= date '2022-03-06' -- report_date from 06-03 lookback
)
,realtime_order_detail as
(
    SELECT
    dot.uid as shipper_id
    -- ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
    --     else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
    -- ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
FROM shopeefood.foody_partner_db__driver_order_tab__reg_continuous_s0_live dot
WHERE 1=1 
AND dot.order_status = 400 -- delivered orders 
AND case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end = date '2022-03-08' -- have order on 08-03 (17h_)

)
select distinct
    o.shipper_id
    ,hub_type
    ,case when ro.shipper_id is not null then 1 else 0 end as is_have_order_83
    ,city_id
    ,max(o.report_date) as last_active_date
from order_detail o
left join 
    (select 
        shipper_id
        ,case when shipper_type_id = 12 then 'Hub' else 'Non-Hub' end as hub_type
        ,city_id
    from shopeefood.foody_mart__profile_shipper_master
    where grass_region = 'VN'
    and try_cast(grass_date as date) = date '2022-03-06' -- replace ngay check shipper_type
    and shipper_type_id <> 12 -- NonHUB
    and city_id in (218,217) -- HN + HCM
        ) as filter
    on o.shipper_id = filter.shipper_id
left join realtime_order_detail as ro
    on o.shipper_id = ro.shipper_id

where hub_type = 'Non-Hub'
and ro.shipper_id is not null

group by 1,2,3,4
having max(report_date) <= date '2022-03-06' - interval '15' day
