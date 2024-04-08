SELECT  base.date_
,base.year_week
-- ,concat(concat(substr(cast(base.year_week as VARCHAR),1,4),'-W'),substr(cast(base.year_week as VARCHAR),5,2)) as year_week_text
-- ,concat(cast(YEAR(base.date_) as VARCHAR),'-','(',date_format(base.date_,'%m'),')',date_format(base.date_,'%b')) as month_
-- ,base.order_source as order_type
,base.city_group
,base.city_name
,base.is_hub 
,base.order_group_type
,base.assign_type
-- ,base.total_single_assign_turn
,base.is_auto_accepted
,base.is_auto_accepted_continuous_assign
,case when base.assign_type = '1. Single Assign' then base.total_single_assign_turn
    else base.assign_type end as assign_type_lv2
 ,count(distinct base.order_uid) as total_order_being_incharged
,sum(total_order) total_order
,sum(if(h_id > 0 or (pick_h_id > 0 and distance <= 2), total_order, 0)) total_order_qualified_hub_less_than_2km

from
(SELECT a.order_uid
,a.order_id
,a.shipper_uid
,case when sm.shipper_type_id = 12 then 1 
      else 0 end as is_hub
,case when a.order_type = 0 then '1. Food/Market'
    when a.order_type = 4 then '2. NowShip Instant'
    when a.order_type = 5 then '3. NowShip Food Mex'
    when a.order_type = 6 then '4. NowShip Shopee'
    when a.order_type = 7 then '5. NowShip Same Day'
    when a.order_type = 8 then '6. NowShip Multi Drop'
    when a.order_type = 200 and ogi.ref_order_category = 0 then '1. Food/Market'
    when a.order_type = 200 and ogi.ref_order_category = 6 then '4. NowShip Shopee'
    when a.order_type = 200 and ogi.ref_order_category = 7 then '5. NowShip Same Day'
    else 'Others' end as order_source
,a.order_type
,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category
,case when a.order_type = 200 then 'Group Order' else 'Single Order' end as order_group_type
,a.city_id
,city.name_en as city_name
,case when a.city_id  = 217 then 'HCM'
    when a.city_id  = 218 then 'HN'
    when a.city_id  = 219 then 'DN'
 	when a.city_id  = 220 then 'Hai Phong'
    when a.city_id  = 221 then 'Can Tho'
 	when a.city_id  = 222 then 'Dong Nai'
    when a.city_id  = 223 then 'Vung Tau'
    when a.city_id  = 230 then 'Binh Duong'
    when a.city_id  = 273 then 'Hue'
 	else 'OTH'
    end as city_group
-- ,a.assign_type as at
,case when a.assign_type = 1 then '1. Single Assign'
      when a.assign_type in (2,4) then '2. Multi Assign'
      when a.assign_type = 3 then '3. Well-Stack Assign'
      when a.assign_type = 5 then '4. Free Pick'
      when a.assign_type = 6 then '5. Manual'
      when a.assign_type in (7,8) then '6. New Stack Assign'
      else null end as assign_type

-- ,a.update_time
-- ,a.create_time
,from_unixtime(a.create_time - 60*60) as create_time
,from_unixtime(a.update_time - 60*60) as update_time
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
      when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
        else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
,a.status
,case when a.experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted
,case when a.experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign
-- ,sa.total_single_assign_turn
,case when sa.total_single_assign_turn = 0 or sa.total_single_assign_turn is null then '# 0'
    when sa.total_single_assign_turn = 1 then '# SA 1'
    when sa.total_single_assign_turn = 2 then '# SA 2'
    when sa.total_single_assign_turn = 3 then '# SA 3'
    when sa.total_single_assign_turn > 3 then '# SA 3+'
    else null end as total_single_assign_turn
,case when a.order_type <> 200 then 1 else coalesce(order_rank.total_order_in_group_at_start,0) end as total_order

,coalesce(hub_info.id,0) as h_id
,coalesce(pick_hub_info.id,0) as pick_h_id
,CAST(dot.delivery_distance AS DOUBLE) / 1000 AS distance

from
    (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group,shipper_uid

    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
    where status in (3,4) -- shipper incharge

    UNION

    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group,shipper_uid

    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    where status in (3,4) -- shipper incharge
    ) a

-- take last incharge
LEFT JOIN
    (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
    where status in (3,4) -- shipper incharge

    UNION

    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    where status in (3,4) -- shipper incharge
    ) a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

-- hub
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot ON dot.ref_order_id = a.order_id and dot.ref_order_category = a.order_type
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet ON dot.id = dotet.order_id
LEFT JOIN
    (SELECT
        id
        , hub_name
        ,case
            when city_id = 217 then 'HCM'
            when city_id = 218 then 'HN'
            when city_id = 219 then 'DN'
        ELSE 'OTH' end as hub_location
    FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
    WHERE 1=1
    and id <> 2
    and driver_count > 0
    ) hub_info on hub_info.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
LEFT JOIN
    (SELECT
        id
        , hub_name as pick_hub_name
        ,case
            when city_id = 217 then 'HCM'
            when city_id = 218 then 'HN'
            when city_id = 219 then 'DN'
        ELSE 'OTH' end as pick_hub_location
    FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
    WHERE 1=1
    and id <> 2
    and driver_count > 0
    ) pick_hub_info on pick_hub_info.id = COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)

-- count # single assign for each order
LEFT JOIN
        (SELECT a.order_uid
            ,count(case when assign_type = 1 then a.order_id else null end) as total_single_assign_turn

        from
            (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

            UNION

            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
            )a

            GROUP By 1
        ) sa on sa.order_uid = a.order_uid

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = a.city_id and city.country_id = 86
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

LEFT JOIN
            (SELECT ogm.group_id
                   ,ogi.group_code
                   ,count (distinct ogm.ref_order_id) as total_order_in_group
                   ,count(distinct case when ogi.create_time = ogm.create_time then ogm.ref_order_id else null end) total_order_in_group_at_start
             FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm

             LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
             WHERE 1=1
             and ogm.group_id is not null

             GROUP BY 1,2
             )order_rank on order_rank.group_id = case when a.order_type = 200 then a.order_id else 0 end
LEFT JOIN (SELECT *,case when grass_date = 'current' then date(current_date)
                         else cast(grass_date as date ) end as report_date 
           from shopeefood.foody_mart__profile_shipper_master   )sm on sm.shipper_id = a.shipper_uid and sm.report_date = date(from_unixtime(a.create_time - 60*60))
where 1=1
and a_filter.order_id is null -- take last incharge
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)base

where base.date_ between current_date - interval '10' day and current_date - interval '1' day
and base.city_name <> ' '

GROUP By 1,2,3,4,5,6,7,8,9,10
