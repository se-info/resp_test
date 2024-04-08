
SELECT
base2.shipper_id
,YEAR(base2.create_date)*100 + WEEK(base2.create_date) as created_year_week
,base2.create_date
,base2.hub_type
,base2.city_name
,coalesce(ado.less_than_min,0) as less_than_min
,coalesce(ado.pass_min,0) as pass_min
,coalesce(ado.over_min,0) as over_min
,coalesce(ado.total_order,0) as total_order 
,case when base2.shipper_id in (19225153,19225507,19225486,19225545,19225776,19226284,19226436,19225794,19248351,19248402,19248500,19248716,19248662,19248672,19248007,19248102,19248225,19248165,19255119,19255179,19255209,19255382,19255482,19255591,19255670,19255693,19255769,19256116,19257707,19257714,19257748,19257761,19258756,19258115,19258127,19258488,19258517,19258524,19258531,19258656,19258926,19258768,19258935,19265444,19265457,19265514,19265625,19265765,19265815,19265869,19265900,19266149,19266154,19266250,19266306,19266323,19266422,19266510,19267854,19267995,19268128,19268347,19268353,19265777,19268587,19268776,19268782,19268938,19269034,19269462,19269068,19269350,19269423,19269607,19269624,19308729,19304698,19307465,19308853,19304701,19307440,19304708,19308760,19305595,19308344,19305592,19308859,19305403,19305602,19309528,19316030,19316027,19315953,19315787,19315917,19315581,19315909,19320293,19307401,19296524,19299647,19299847,19309246,19309349,19315658,19315950,19318765,19318784,19318750,19319042,19319298,19319421,19319886,19325248,19325554,19325298,19329174)
then 'Fresh' else 'Pt16' end as rec_source
,sum(base2.total_online_time) as total_online_time
,sum(base2.total_working_time) as total_working_time
,sum(base2.in_shift_online_time) as in_shift_online_time
,sum(base2.in_shift_work_time) as in_shift_work_time
FROM 
(SELECT base.shipper_id
,base.create_date
,base.hub_type
,base.city_name
-- total
,date_diff('second',base.actual_start_time_online,base.actual_end_time_online)*1.0000/(60*60) as total_online_time
,date_diff('second',base.actual_start_time_work,base.actual_end_time_work)*1.0000/(60*60) as total_working_time


--- shift
,case when base.actual_end_time_online < base.start_shift_time then 0
    when base.actual_start_time_online > base.end_shift_time then 0
    else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_online)   ,   least(base.end_shift_time,base.actual_end_time_online)   )*1.0000/(60*60)
    end as in_shift_online_time

,case when base.actual_end_time_work < base.start_shift_time then 0
    when base.actual_start_time_work > base.end_shift_time then 0
    else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_work)   ,   least(base.end_shift_time,base.actual_end_time_work)   )*1.0000/(60*60)
    end as in_shift_work_time
from
(SELECT uid as shipper_id
,shift.off_weekdays
,shift.city_name
,case when date_format(from_unixtime(create_time - 60*60),'%a') in ('Mon') then 1
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Tue') then 2
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Wed') then 3
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Thu') then 4
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Fri') then 5
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Sat') then 6
when date_format(from_unixtime(create_time - 60*60),'%a') in ('Sun') then 7
end as day_of_week
,date(from_unixtime(create_time - 60*60)) as create_date
,CASE WHEN shift.shift_hour = 10 then 'HUB10GIO'
WHEN shift.shift_hour = 8 then ' HUB8GIO'
WHEN shift.shift_hour = 5  and shift.start_time < '11' then 'HUB5GIOS'
WHEN shift.shift_hour = 5  and shift.start_time > '11' then 'HUB5GIOC'
END as hub_type
-- important timestamp
,from_unixtime(check_in_time - 60*60) as check_in_time
,from_unixtime(check_out_time - 60*60) as check_out_time
,from_unixtime(order_start_time - 60*60) as order_start_time
,from_unixtime(order_end_time - 60*60) as order_end_time

-- for checking
,check_in_time as check_in_time_original
,check_out_time as check_out_time_original
,order_start_time as order_start_time_original
,order_end_time as order_end_time_original
------------------

,total_online_seconds*1.00/(60*60) as total_online_hours
,(check_out_time - check_in_time)*1.00/(60*60) as online
,total_work_seconds*1.00/(60*60) as total_work_hours
,(order_end_time - order_start_time)*1.00/(60*60) as work

-- actual use
,from_unixtime(check_in_time - 60*60) as actual_start_time_online
,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online

,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work

,date_add('hour',shift.start_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as start_shift_time
,date_add('hour',shift.end_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as end_shift_time


from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts

left join 
        (SELECT sm.shipper_id
            ,case when grass_date = 'current' then date(current_date)
                else cast(grass_date as date) end as report_date
            ,sm.shipper_shift_id
            ,sm.city_id
            ,sm.city_name
            ,sm.shipper_type_id
            ,ss.off_weekdays
            ,case when (date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600)))) = 8 then (ss.start_time + 300)/3600
            else ss.start_time/3600 end as start_shift
            --,ss.start_time/3600 as start_shift
            ,ss.end_time/3600 as end_shift
            ,date_format(from_unixtime(start_time - 25200),'%H') as start_time
            ,date_format(from_unixtime(end_time - 25200),'%H') as end_time
            ,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss on ss.id = sm.shipper_shift_id
            where 1=1
            and sm.grass_region = 'VN'
            
        )shift on shift.shipper_id = sts.uid and shift.report_date = date(from_unixtime(create_time - 60*60))
where 1=1
and shift.shipper_type_id = 12
AND shift.city_id in (217,218)
and date(from_unixtime(create_time - 60*60)) between date('2019-01-01') and date(now() - interval '1' day) -- date('2020-06-30')
and check_in_time > 0
and check_out_time > 0
and check_out_time >= check_in_time
and order_end_time >= order_start_time
and ((order_start_time = 0 and order_end_time = 0)
    OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)))base
where 1=1 
and regexp_like(base.off_weekdays,cast(base.day_of_week as VARCHAR)) = false   )base2


---- ADO 
LEFT JOIN (SELECT base1.report_date
,base1.created_year_week
,base1.uid
,base1.shipper_name
,hi.hub_name
,base1.city_group
,base1.hub_type
,base1.total_order
,base1.total_weekly_bonus
--,base1.total_ship_shared
,base1.ship_shared
,base1.daily_bonus
,base1.bad_weather
,base1.surge_fee
,case when base1.ship_shared < base1.total_ship_shared then base1.total_ship_shared - base1.ship_shared
ELSE 0 end as extra_ship
,(base1.total_ship_shared + base1.daily_bonus) as total_income
,case WHEN base1.hub_type = 'HUB5GIOS' AND base1.total_order < 16 THEN 1
WHEN base1.hub_type = 'HUB5GIOC' AND base1.total_order < 16 THEN 1
WHEN base1.hub_type = 'HUB8GIO' AND base1.total_order < 25 THEN 1
WHEN base1.hub_type = 'HUB10GIO' AND base1.total_order < 30 THEN 1
ELSE 0 END as less_than_min
,case WHEN base1.hub_type = 'HUB5GIOS' AND base1.total_order > 16 THEN 1
WHEN base1.hub_type = 'HUB5GIOC' AND base1.total_order > 16 THEN 1
WHEN base1.hub_type = 'HUB8GIO' AND base1.total_order > 25 THEN 1
WHEN base1.hub_type = 'HUB10GIO' AND base1.total_order >30 THEN 1
ELSE 0 END AS over_min
,case WHEN base1.hub_type = 'HUB5GIOS' AND base1.total_order = 16 THEN 1
WHEN base1.hub_type = 'HUB5GIOC' AND base1.total_order = 16 THEN 1
WHEN base1.hub_type = 'HUB8GIO' AND base1.total_order = 25 THEN 1
WHEN base1.hub_type = 'HUB10GIO' AND base1.total_order = 30 THEN 1
ELSE 0 END AS pass_min
FROM
(SELECT base.report_date
,base.created_year_week
,base.city_group
,base.uid
,base.hub_type
,base.shipper_name
,(13500* base.total_order) as ship_shared
,base.total_order as total_order
--,count(base.uid) as total_active
,sum(case when base.bwf > 0 then base.bwf else 0 end) as bad_weather
,sum(case when base.txn_type in ('Daily_Bonus') then base.balance ELSE 0 END) as daily_bonus
,sum(case when base.txn_type in ('Ship_shared') then base.balance ELSE 0 END) as total_ship_shared
,sum(case when base.txn_type in ('Weekly_Bonus') then base.balance ELSE 0 END) as total_weekly_bonus
,sum(case when base.txn_type in ('Surge_fee') then base.balance ELSE 0 END) as surge_fee
--,sum(case when base.txn_type in ('Ship_shared') then base.balance ELSE NULL END)  as Total_income
FROM

(SELECT oct.shipper_uid as uid
,psm.shipper_name
,case
    WHEN oct.city_id = 217 then 'HCM'
    WHEN oct.city_id = 218 then 'HN'
    ELSE NULL end as city_group
,date(from_unixtime(oct.final_delivered_time -3600)) as report_date
,YEAR(date(from_unixtime(oct.final_delivered_time -3600)))*100 + WEEK(date(from_unixtime(oct.final_delivered_time -3600))) as created_year_week
,case
      WHEN sst.shift_hour = 5 and sst.start_time < '11' then 'HUB5GIOS'
      WHEN sst.shift_hour = 5 and sst.start_time > '11' then 'HUB5GIOC'
      WHEN sst.shift_hour = 8 then 'HUB8GIO'
      WHEN sst.shift_hour = 10 then 'HUB10GIO'
      
      ELSE NULL END AS hub_type
,case WHEN txn.txn_type = 906 then 'Ship_shared'
WHEN txn.txn_type = 907 then 'Daily_Bonus'
when txn.txn_type = 505 then 'Weekly_Bonus'
when txn.txn_type = 518 then 'Surge_fee'
ELSE NULL END AS txn_type
,txn.balance
--,(13500 * count(oct.id)) as ship_shared
--,count(distinct oct.shipper_uid) as total_active_driver
,sum(cast(json_extract(order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as bigint )) as bwf --badweatherfee
,count(oct.id) as total_order
--,oct.id as order_id
FROM shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct

LEFT JOIN 
        (
        select *
        ,Case
        when grass_date = 'current' then date(current_date)
        else cast(grass_date AS DATE ) END as report_date
        from shopeefood.foody_mart__profile_shipper_master
        ) psm on psm.shipper_id = oct.shipper_uid AND psm.report_date = date(from_unixtime(oct.final_delivered_time -3600))

LEFT JOIN shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on oct.id = dot.ref_order_id 
LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live doet on dot.id = doet.order_id 
LEFT JOIN ( SELECT 
id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live) sst on sst.id = psm.shipper_shift_id
LEFT JOIN (SELECT date(from_unixtime(create_time - 90000))  as report_date --,date_format(from_unixtime(create_time -3600),'%H:%i:%S') as time_create 
, user_id,txn_type ,balance/100 as balance ,previous_balance/100 as previous_balance
FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
WHERE 1=1 
--AND date(from_unixtime(create_time - 3600)) = date('2021-04-20')
AND txn_type in (906,907,505,518)
) txn ON txn.user_id = oct.shipper_uid and txn.report_date  =  date(from_unixtime(oct.final_delivered_time -3600))
WHERE 1=1
AND oct.city_id in (217,218)
AND cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2
AND psm.shipper_type_id in (12)
--AND txn.txn_type in (906,907)
AND oct.status in(7,9)
AND date(from_unixtime(oct.final_delivered_time -3600)) >= date((current_date) - interval '30' day)
and date(from_unixtime(oct.final_delivered_time -3600)) < date(current_date)
 --AND date(from_unixtime(oct.final_delivered_time -3600)) = date('2021-06-01') 
GROUP by 1,2,3,4,5,6,7,8 )base
GROUP BY 1,2,3,4,5,6,7,8)base1
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_daily_s0_live hs ON base1.uid = hs.uid 
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi ON hi.id = hs.hub_id )ado on ado.uid = base2.shipper_id and ado.report_date = base2.create_date 

WHERE 1=1 
and base2.create_date >= date((current_date)-interval '1'day)
and base2.create_date < date(current_date)
GROUP BY 1,2,3,4,5,6,7,8,9
