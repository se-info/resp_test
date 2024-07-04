with raw as 
(select 
         dot.uid  
       ,sm.shipper_name
       ,sm.city_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
       ,dot.ref_order_code
       ,case when ref_order_category = 0 then rate.shipper_rate else null end as rating 
       ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date 
       ,date_format(date(from_unixtime(dot.real_drop_time - 3600)),'%a') as days_of_week


from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))



LEFT JOIN
(SELECT order_id
,shipper_uid as shipper_id
,case when cfo.shipper_rate = 0 then null
when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
when cfo.shipper_rate = 104 then 4
when cfo.shipper_rate = 105 then 5
else null end as shipper_rate
,from_unixtime(cfo.create_time - 60*60) as create_ts

FROM shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
)rate ON dot.ref_order_id = rate.order_id and dot.uid = rate.shipper_id



where 1 = 1 
and dot.order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) between date'2022-04-01' and date'2022-04-30'
and date(from_unixtime(dot.real_drop_time - 3600)) != date'2022-04-04'
)

,driver as 

(
    select  
        a.uid 
        -- ,case when a.days_of_week in ('Sun','Sat') then 'Weekends'
        --       when a.days_of_week not in ('Sun','Sat') then 'Weekdays' else null end as date_type
       ,'All days' as date_type       
       ,a.shipper_name
       ,a.city_name
       ,2022 - cast(substr(cast(ps.birth_date as varchar),1,4) as bigint) as age_
       ,ps.gender
       ,working_group
       ,date(from_unixtime(ps.create_time - 3600)) as onboard_date
       ,count(distinct a.ref_order_code) as total_order
       ,sum(a.rating)*1.00/count(distinct case when a.rating > 0 then a.ref_order_code else null end)  as avg_rating 
       ,count(distinct a.report_date) as working_day
    --    ,sum(inc.total_earning_before_tax) as total_income





from raw a 


left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live ps on ps.uid = a.uid


where working_group = 'Non Hub'

group by 1,2,3,4,5,6,7,8
)


select 
        a.uid as shipper_id 
        ,a.shipper_name
        ,a.date_type
        ,a.city_name
        ,a.age_
        ,a.gender
        ,a.working_group
        ,a.onboard_date
        -- ,a.total_order
        ,a.avg_rating
        ,a.working_day
       ,sum(inc.total_earning_before_tax) as total_income
        

        from driver a 
        left join (select * from vnfdbi_opsndrivers.snp_foody_shipper_income_tab 
                   where date_ between date'2022-04-01' and date'2022-04-30' )inc on inc.partner_id = a.uid



group by 1,2,3,4,5,6,7,8,9,10
