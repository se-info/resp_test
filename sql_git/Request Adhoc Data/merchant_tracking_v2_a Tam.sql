with assignment as 
(select a.date_
    --    ,a.order_id
       ,oct.restaurant_id
       ,count(case when a.issue_category = 'Ignore' then a.order_id else null end) as total_ignore
       ,count(case when a.issue_category != 'Ignore' then a.order_id else null end) as total_denied


from    
(select order_id 
       ,issue_category
       ,shipper_id
       ,order_code
       ,date_ 

from dev_vnfdbi_opsndrivers.phong_raw_assignment_test 

where order_code = 0 

and date_ between date'2022-06-26' and date'2022-07-10'
) a 

left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = a.order_id

-- left join shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = oct.restaurant_id and try_cast(mm.grass_date as date) = a.date_

-- where oct.restaurant_id = 47293

group by 1,2
)



select 
        a.date_
        ,'All' as created_hour 
        ,a.city_name
        ,mex_id
        ,a.merchant_name
        ,service
        ,sum(cnt_cancel_no_driver) as cnt_cancel_no_driver
        ,sum(total_submit) as gross 
        ,sum(total_net) as net
        ,sum(prep_time)/cast(count(distinct created_hour)as double) as avg_prep 
        ,sum(avg_incharge_time)/cast(count(distinct created_hour)as double) as avg_incharge_time 
        ,sum(total_item_net_order)/cast(sum(total_net) as double) as avg_item
        ,total_ignore
        ,total_denied
        ,b.district_name


from  dev_vnfdbi_opsndrivers.merchant_level_data a 

left join assignment b on a.mex_id = b.restaurant_id and a.date_ = b.date_

-- left join dev_vnfdbi_opsndrivers.phong_raw_assignment_test ra on ra.

left join shopeefood.foody_mart__profile_merchant_master b on b.merchant_id = a.mex_id and b.grass_date = 'current'

where a.date_ between date'2022-06-26' and date'2022-07-10'

and mex_id in(632238
,1114973
,133342
,198605
,908398
,972021
,647143
,871083
,977745
,47294
,950380
,47293
,91266
,120212
,704702
,247390
,1114974
,645116
,1037991
,196972
,1132427
,1127746
,297026
,972974
,300450)


group by 1,2,3,4,5,6,12,13,14

order by 4,1