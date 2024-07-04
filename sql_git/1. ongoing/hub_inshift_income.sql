-- select * from driver_ops_hub_driver_performance_tab
-- where total_order > 0 
with sunday_bonus as
(select 
                  user_id,
                  split(trim(note),' ')[9] as note_split,
                  cast(date_parse(split(trim(note),' ')[9]||'/'||'24','%d/%m/%y') as date) as start_date,
                  cast(date_parse(split(trim(note),' ')[11]||'/'||'24','%d/%m/%y') as date) as end_date,
                  balance/cast(100 as double) as value_bonus   


        from (select * ,case when user_id in (20754253,6872389,21722008,18521518,18448800,21634564,7999552,22285257) and create_time = 1655866978 then 0 else 1 end as is_valid
              from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
              where 1 = 1 
              and txn_type = 520
              and date(from_unixtime(create_time - 3600)) >= current_date - interval '30' day
              and note like '%HUB_MODEL_Thuong tai xe guong mau chu nhat tuan%'  
             )
        where 1 = 1  
        and is_valid = 1 
)
,hub_inshift as 
(select 
        hp.date_,
        hp.uid,
        hp.city_name,
        coalesce(s.value_bonus,0) as sunday_bonus,
        sum(total_order) as total_order,
        sum(total_income) as total_income,
        sum(total_order)*13500 as ship_shared,
        sum(daily_bonus) as daily_bonus,
        sum(extra_ship) as extra_ship,
        sum(greatest(in_shift_online_time,in_shift_work_time)) as online_time

from driver_ops_hub_driver_performance_tab hp 

left join sunday_bonus as s on s.user_id = hp.uid and s.end_date = hp.date_

where hp.total_order > 0 
and hp.date_ between date'2024-04-08' and date'2024-04-14'
and hp.city_name in ('HCM City','Ha Noi City')
group by 1,2,3,4)
select  
        date_,
        'HCM & HN' as city_name,
        count(distinct uid) as a1,
        sum(total_order) as total_order,
        sum(ship_shared) as ship_shared,
        sum(daily_bonus) as daily_bonus,
        sum(extra_ship) as extra_ship,
        sum(sunday_bonus) as sunday_bonus,
        sum(online_time) as online_time

from hub_inshift
group by 1,2
;
-- out shift and non hub income
with raw as 
(select * from driver_ops_driver_performance_tab
where shipper_tier != 'Hub'
and total_order > 0
-- and hub_order = 0 
and report_date between date'2024-04-08' and date'2024-04-14'
and city_name in ('HCM City','Ha Noi City')
)
select 
        report_date,
        'HCM & HN' as  city_name,
        count(distinct shipper_id) as a1,
        sum(online_hour) as online_hour,
        sum(driver_income - driver_other_income - driver_daily_bonus) as ship_shared,
        sum(driver_daily_bonus) as driver_daily_bonus
from raw

group by 1,2


