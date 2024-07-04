WITH log_hub as
            (
            SELECT uid as shipper_id , change_type, ref_id as hub_id, from_unixtime(create_time - 60*60) as create_ts, create_uid
                   , case when uid in (8618999,2996387,3437959,16513079,13183218,19269423,14012684,18718560,4826244,10403079) and date(from_unixtime(create_time - 60*60)) between date('2021-09-20') and date('2021-09-30') then 1
                          when uid in (15213402) and ref_id not in (26,35) and date(from_unixtime(create_time - 60*60)) between date('2021-09-20') and date('2021-09-30') then 1
                          when uid in (3805328) and date(from_unixtime(create_time - 60*60)) between date('2021-09-21') and date('2021-09-30') then 1
                          else 0 end as is_test
            FROM
                    (
                    SELECT uid, change_type, ref_id, create_time, create_uid
                    FROM shopeefood.foody_internal_db__shipper_change_log_tab__reg_daily_s0_live
                    WHERE change_type in (4,5)

                    UNION

                    SELECT uid, change_type, ref_id, create_time, create_uid
                    FROM shopeefood.foody_partner_db__shipper_change_log_tab__reg_daily_s0_live
                    WHERE change_type in (4,5)
                    )log_hub

            WHERE 1=1
            GROUP BY 1,2,3,4,5
            )
    , log_hub_v1 as
        (
        SELECT *
        FROM
                (
                SELECT shipper_id, change_type, hub_id, create_ts, create_uid
                      ,row_number() over(partition by concat(cast(shipper_id as VARCHAR),cast(change_type as varchar),cast(date(create_ts) as varchar)) order by create_ts desc) row_num
                FROM log_hub
                WHERE change_type = 4 and is_test = 0

                UNION

                SELECT shipper_id, change_type, hub_id, create_ts, create_uid
                      ,row_number() over(partition by concat(cast(shipper_id as VARCHAR),cast(change_type as varchar),cast(date(create_ts) as varchar)) order by create_ts asc) row_num
                FROM log_hub
                WHERE change_type = 5 and is_test = 0
                )
        WHERE row_num = 1
        )
    , revised_log as
            (
            SELECT shipper_id
                  ,hub_current_start
                  ,hub_current_join_time
                  ,hub_current_end
                  ,hub_curent_drop_time

            FROM
            (
            (       SELECT * FROM
                    (
                    SELECT t1.shipper_id
                          ,t1.change_type as change_type_from
                          ,t1.hub_id as hub_current_start
                          ,t1.create_ts as hub_current_join_time

                          ,t2.change_type as change_type_to
                          ,case when t2.hub_id > 0 then t2.hub_id else t1.hub_id end as hub_current_end
                          ,case when t2.hub_id > 0 then t2.create_ts else localtimestamp - interval '1' hour end as hub_curent_drop_time

                          ,row_number() over(partition by concat(cast(t1.shipper_id as varchar),cast(t1.hub_id as varchar),cast(t1.create_ts as varchar)) order by t2.create_ts ASC ) rank
                    FROM (SELECT * FROM log_hub_v1 WHERE change_type = 4) t1

                    LEFT JOIN (SELECT * FROM log_hub_v1 WHERE change_type = 5) t2 on t1.shipper_id = t2.shipper_id and t1.create_ts < t2.create_ts and t1.hub_id = t2.hub_id
                    WHERE 1=1
                    )base

                    WHERE rank = 1

            )

            -------
            UNION

            (       SELECT * FROM
                    (
                    SELECT t1.shipper_id
                          ,t2.change_type as change_type_from

                          ,case when t2.hub_id > 0 then t2.hub_id else t1.hub_id end as hub_current_start
                          ,case when t2.hub_id > 0 then t2.create_ts else cast(date('2021-05-27') as TIMESTAMP) end as hub_current_join_time

                          ,t1.change_type as change_type_to
                          ,t1.hub_id as hub_current_end
                          ,t1.create_ts as hub_curent_drop_time

                          ,row_number() over(partition by concat(cast(t1.shipper_id as varchar),cast(t1.hub_id as varchar),cast(t1.create_ts as varchar)) order by t2.create_ts DESC ) rank

                    FROM (SELECT * FROM log_hub_v1 WHERE change_type = 5) t1

                    LEFT JOIN (SELECT * FROM log_hub_v1 WHERE change_type = 4) t2 on t1.shipper_id = t2.shipper_id and t1.create_ts > t2.create_ts and t1.hub_id = t2.hub_id
                    )base1

                    WHERE rank = 1

            )
            )base


            WHERE 1=1 and shipper_id IN (9954638, 4750302, 18774323)

            GROUP BY 1,2,3,4,5
            )
SELECT 
    shipper_id
    , COUNT(DISTINCT hub_current_join_time) AS join_hub_time 
FROM 
    revised_log 
GROUP BY 
    1