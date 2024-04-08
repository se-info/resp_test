with denied_ignore as 
(select 
        date_ 
       ,shipper_id
       ,case when issue_category = 'Ignore' then 'Ignore' else 'Denied' end as order_status 
       ,order_id
       ,order_type
       ,timestamp as order_timestamp 


from phong_raw_assignment_test
)
,raw_order as 
(select 
         date(from_unixtime(real_drop_time - 3600)) as date_ 
        ,uid as shipper_id 
        ,'Delivered' as order_status
        ,ref_order_id as order_id 
        , CASE
                WHEN ref_order_category = 0 THEN '1. Food/Market'
                WHEN ref_order_category in (4,5) THEN '2. NS'
                WHEN ref_order_category = 6 THEN '3. NSS'
                WHEN ref_order_category = 7 THEN '4. NS Same Day'
                ELSE 'Others' END AS order_type
            ,from_unixtime(real_drop_time - 3600) as order_timestamp 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day)

where order_status = 400
)
,final_metrics as 
(
select *

from denied_ignore

UNION ALL 

select *

from raw_order

)
select
        a.date_
       ,a.shipper_id
       ,order_type
       ,order_status 
       ,date(from_unixtime(pt.create_time - 3600)) as onboard_date
       ,count(distinct a.order_id) as total_order_delivered   



from final_metrics a 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live pt on pt.uid = a.shipper_id 

where a.shipper_id in 
(21746571
,40001238
,40060380
,23176547
,16743695
,18680672)

group by 1,2,3,4,5
