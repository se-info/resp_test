SELECT *
        
        FROM 
                (
                SELECT ht.id as ticket_id 
                    ,tot.order_code
                      ,case when ht.status = 1 then '1. Open'
                            when ht.status = 2 then '2. Pending'
                            when ht.status = 3 then '3. Resolved'
                            when ht.status = 5 then '4. Completed'
                            when ht.status = 4 then '5. Closed'
                            else null end as status
                      ,case when ht.incharge_team = 1 then 'CC'
                            when ht.incharge_team = 2 then 'PROJECTOR'
                            when ht.incharge_team = 3 then 'EDITOR'
                            when ht.incharge_team = 4 then 'GOFAST'
                            when ht.incharge_team = 5 then 'PRODUCT SUPPORT'
                            when ht.incharge_team = 6 then 'AGENT'
                            when ht.incharge_team = 7 then 'AGENT MANAGER'
                            else null end as incharge_team 
                      ,case when ht.ticket_type = 1 then 'VIOLATION_OF_RULES'
                            when ht.ticket_type = 2 then 'CHANGE_SHIPPER_INFO'
                            when ht.ticket_type = 3 then 'FRAUD'
                            when ht.ticket_type = 4 then 'CUSTOMER_FEEDBACK'
                            when ht.ticket_type = 5 then 'CC_FEEDBACK'
                            when ht.ticket_type = 6 then 'NOW_POLICE'
                            when ht.ticket_type = 7 then 'MERCHANT_FEEDBACK'
                            when ht.ticket_type = 8 then 'PARTNER_SIGNATURE_NOTE'
                            when ht.ticket_type = 9 then 'REQUEST_CHANGE_DRIVER_INFO'
                            else null end as ticket_type
                      
                      ,case when ht.city_id = 217 then 'HCM'
                            when ht.city_id = 218 then 'HN'
                            when ht.city_id = 219 then 'DN'
                            ELSE 'OTH' end as city_group
                      ,from_unixtime(ht.create_time - 60*60) as created_timestamp
                      --,Extract(HOUR from from_unixtime(ht.create_time - 60*60)) created_hour
                      --,date(from_unixtime(ht.create_time - 60*60)) created_date
                      --,case when cast(from_unixtime(ht.create_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                        --    when cast(from_unixtime(ht.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                          --  when cast(from_unixtime(ht.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                            --else YEAR(cast(from_unixtime(ht.create_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(ht.create_time - 60*60) as date)) end as created_year_week
                      ,COALESCE(htl.label,'NO_ACTION') resolution
                      ,case when ht.resolve_time > 0 then from_unixtime(ht.resolve_time - 60*60)
                      WHEN ht.update_time > 0 then from_unixtime(ht.update_time - 60*60)
                      else null end as resolve_timestamp
                      ,date_diff('second',from_unixtime(ht.create_time - 60*60), case when ht.resolve_time > 0 then from_unixtime(ht.resolve_time - 60*60) else from_unixtime(ht.update_time - 60*60) end) lt_resolve
                      ,htu.uid as shipper_id 
                      ,sm.shipper_name
                      --,json_extract(ht.extra_data,'$.reporter') as created_by
                    , cast(json_extract(ht.extra_data, '$.reporter')as varchar) created_by
   ,concat('Note', coalesce(trim(CAST(json_extract(ht.extra_data, '$.description') AS varchar)),'N/A')) description
                      
                FROM foody_internal_db__hr_tick_tab ht 
                LEFT JOIN foody_internal_db__hr_tick_label_tab htl on htl.tick_id = ht.id
                LEFT JOIN foody_internal_db__hr_tick_user_tab htu on htu.tick_id = ht.id
                LEFT JOIN foody_mart__profile_shipper_master sm on sm.shipper_id = htu.uid and sm.grass_date = 'current'
                LEFT JOIN foody.foody_internal_db__hr_tick_order_tab tot ON (ht.id = tot.tick_id)
                WHERE 1=1
                and ht.incharge_team = 4
                and date(from_unixtime(ht.create_time - 60*60)) >= date(current_date) - interval '7' day
                and date(from_unixtime(ht.create_time - 60*60)) < date(current_date)
)