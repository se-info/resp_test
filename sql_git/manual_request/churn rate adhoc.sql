with driver_list as 
(select distinct
                year(try_cast(grass_date as date))*100+month(try_cast(grass_date as date)) as month_year
               ,shipper_id
               ,city_name
               ,max(try_cast(grass_date as date)) as last_date
 

       from  shopeefood.foody_mart__profile_shipper_master
        -- where shipper_id = 4826244
        where try_cast(grass_date as date) >= date'2022-01-01'
        and shipper_status_code = 1    
        group by 1,2,3

)
-- params_date(month_year,start_date,end_date) as 
-- (
-- VALUES
--        (202201,date'2022-01-01',date'2022-01-31')
--       ,(202202,date'2022-02-01',date'2022-02-28')
--       ,(202203,date'2022-03-01',date'2022-03-31')
--       ,(202204,date'2022-04-01',date'2022-04-30')  
--       ,(202205,date'2022-05-01',date'2022-05-31')
--       ,(202206,date'2022-06-01',date'2022-06-30')
--       ,(202207,date'2022-07-01',date'2022-07-31')
--       ,(202208,date'2022-08-01',date'2022-08-31')
--       ,(202209,date'2022-01-01',current_date - interval '1' day)  
-- )
,driver_order as 
(SELECT 
        date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        -- ,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
        ,year(date(from_unixtime(dot.real_drop_time - 3600)))*100+month(date(from_unixtime(dot.real_drop_time - 3600))) as year_month
        ,dot.uid as driver_id
        , count(dot.ref_order_code) as total_order
        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

where dot.order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) >= date'2021-12-01'

group by 1,2,3
)
,metrics as 
(select 
       sm.month_year
      ,sm.shipper_id as driver_id
      ,sm.city_name
      ,month(from_unixtime(iw.create_time - 3600)) as onboard_month
      ,case when year(from_unixtime(iw.create_time - 3600))*100+month(from_unixtime(iw.create_time - 3600)) < sm.month_year then 1 else 0 end as is_valid_churn
      ,array_agg(distinct do.year_month) as delivered_month
      ,CARDINALITY(FILTER(array_agg(distinct do.year_month),x -> x = sm.month_year - 1)) > 0  as rule_2_active_in_l1t
      ,CARDINALITY(FILTER(array_agg(distinct do.year_month),x -> x = sm.month_year)) > 0  as rule_2_active_in_t 

from driver_list sm 

left join shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live iw on iw.uid = sm.shipper_id

left join driver_order do on do.driver_id = sm.shipper_id

group by 1,2,3,4,5
)
,final as 
(select 
        *
       ,CASE WHEN rule_2_active_in_l1t = true and rule_2_active_in_t = false then 'churn-driver' 
             WHEN rule_2_active_in_l1t = false and rule_2_active_in_t = false then 'inactive-driver'
             else 'normal' end as driver_type 

from metrics 
-- where driver_id = 4826244
)
-- select * from final
select 
        month_year
       ,city_name
       ,count(distinct driver_id) as total_existing_driver
       ,count(distinct case when driver_type = 'normal' then driver_id else null end) as total_normal_driver
       ,count(distinct case when driver_type = 'inactive-driver' then driver_id else null end) as total_inactive_driver
       ,count(distinct case when driver_type = 'churn-driver' and is_valid_churn = 1 then driver_id else null end) as total_churn_driver
from final 

group by 1,2






