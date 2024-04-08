with base_assignment as
(
select
date(create_timestamp) created_date
        ,ref_order_category
        ,order_uid
        ,shipper_uid
        ,create_timestamp
        ,status
        ,case
        when regexp_like(lower(city.name_en),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
        when raw.city_id in (217,218,219) then city.name_en
        when raw.city_id in (220,221,222,223,230,273) then 'T2'
        else 'T3' end as city_tier

from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = raw.city_id
    and city.country_id = 86

where status in (3,4,2,14,15,8,9,17,18) 
and date(create_timestamp) between date'2023-09-01' and date'2023-09-30' 
)



select 
        case 
        when ref_order_category = 0 then 'Food'
        when ref_order_category != 0 then 'Ship'
        end as service,
        coalesce(city_tier,'VN') as cities,
        count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end)*1.000/count(distinct created_date) as no_assign,
        count(distinct case when status in (3,4) then (shipper_uid,order_uid,create_timestamp) else null end)*1.000/count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_incharged,
        count(distinct case when status in (8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end)*1.000/count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_ignored,
        count(distinct case when status in (2,14,15) then (shipper_uid,order_uid,create_timestamp) else null end)*1.000/count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_deny

from base_assignment
group by 1, grouping sets (city_tier,())
order by 3 desc 


