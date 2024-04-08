with pool_driver as
(select 
    *
from vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab
where report_date = date '2022-04-15'
)
,base_dod as
(
SELECT
    dod.uid AS shipper_id
    , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
    , dot.ref_order_id
    , dot.ref_order_code
    , CASE
        WHEN dot.ref_order_category = 0 THEN 'Food/Market'
        WHEN dot.ref_order_category = 4 THEN 'NS Instant'
        WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
        WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
        WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
        WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
    ELSE NULL END AS order_source
    , CASE
        WHEN dod.deny_type = 0 THEN 'NA'
        WHEN dod.deny_type = 1 THEN 'Driver_Fault'
        WHEN dod.deny_type = 10 THEN 'Order_Fault'
        WHEN dod.deny_type = 11 THEN 'Order_Pending'
        WHEN dod.deny_type = 20 THEN 'System_Fault'
    END AS deny_type
    , reason_text
    -- ,qualified.is_order_qualified_hub
    -- ,qualified.created_date as order_created_date
    -- ,qualified.source
    ,FROM_UNIXTIME(dod.create_time - 3600) as deny_timestamp
    ,date_format(FROM_UNIXTIME(dod.create_time - 3600),'%T') as hour_timestamp
    ,cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60 as hour_minute

    FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
    -- LEFT JOIN driver_hub on driver_hub.shipper_id = dod.uid and driver_hub.report_date = DATE(FROM_UNIXTIME(dod.create_time - 3600))
    -- left join order_qualified_base qualified on dot.ref_order_id = qualified.order_id
    WHERE 1=1
    and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = date '2022-04-15'
    -- and dot.ref_order_category = 0
    )

,base_assign as
(SELECT
    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
AND grass_schema = 'foody_partner_archive_db'

UNION ALL

SELECT
    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
AND schema = 'foody_partner_db'
)
,base_assign1 as
(SELECT
    a.order_uid
    , a.order_id
    , CASE
        WHEN a.order_type = 0 THEN '1. Food/Market'
        WHEN a.order_type in (4,5) THEN '2. NS'
        WHEN a.order_type = 6 THEN '3. NSS'
        WHEN a.order_type = 7 THEN '4. NS Same Day'
    ELSE 'Others' END AS order_type
    , a.order_type AS order_code
    , a.city_id
    , city.name_en AS city_name
    , CASE
        WHEN a.city_id  = 217 THEN 'HCM'
        WHEN a.city_id  = 218 THEN 'HN'
        WHEN a.city_id  = 219 THEN 'DN'
        WHEN a.city_id  = 220 THEN 'HP'
        ELSE 'OTH'
    END AS city_group
    ,CASE
        WHEN a.assign_type = 1 THEN '1. Single Assign'
        WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
        WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
        WHEN a.assign_type = 5 THEN '4. Free Pick'
        WHEN a.assign_type = 6 THEN '5. Manual'
        WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
    ELSE NULL END AS assign_type
    , DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
    , CASE
        WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) >= 52 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))-1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
        WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) = 1 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))+1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
    ELSE YEAR(DATE(from_unixtime(a.create_time - 3600)))*100 + WEEK(DATE(from_unixtime(a.create_time - 3600))) END AS year_week
    , a.status
    , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
    , a.shipper_id
    -- , IF(dod.deny_type = 1, 1, 0) AS is_deny_driver_fault
    -- , case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
    -- ,qualified.is_order_qualified_hub
    -- ,qualified.created_date as order_created_date
    -- ,qualified.source
    ,FROM_UNIXTIME(a.create_time - 3600) as create_timestamp
    ,cast(hour(FROM_UNIXTIME(a.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(a.create_time - 3600)) as double)/60 as hour_minute

FROM base_assign a

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = a.city_id AND city.country_id = 86
-- LEFT JOIN dod ON a.order_id = (CASE WHEN dod.ref_order_category <> 7 THEN dod.ref_order_id
--                                         WHEN dod.ref_order_category = 7 AND dod.group_id = 0 THEN dod.ref_order_id
--                                         ELSE dod.group_id END) AND a.order_type = dod.ref_order_category AND a.shipper_id = dod.uid
-- LEFT JOIN driver_hub on driver_hub.shipper_id = a.shipper_id and driver_hub.report_date = DATE(FROM_UNIXTIME(a.create_time - 3600))
-- left join order_qualified_base qualified on a.order_id = qualified.order_id
WHERE 1=1
AND DATE(FROM_UNIXTIME(a.create_time - 3600)) = date '2022-04-15'
and status IN (8,9,17,18)
-- and a.order_type = 0
-- AND qualified.created_date = date '2022-04-04'
)

,deny_ignore as
(select 
    p.*
    ,deny.ref_order_id
    -- ,deny.ref_order_code
    -- ,deny.hour_timestamp
    ,deny.deny_date
    ,deny.order_source
    -- ,deny.deny_type
    ,deny.hour_minute
    ,deny_type as type_deny_ignore

from pool_driver p
left join base_dod deny
    on p.shipper_id = deny.shipper_id

where deny.hour_minute not between 11 and 11.5

union all

select 
    p.*
    ,ignore.order_id
    ,ignore.date_
    ,ignore.order_type as ourder_source
    
    -- ,deny.ref_order_code
    -- ,deny.hour_timestamp
    -- ,deny.deny_date
    -- ,deny.order_source
    -- ,deny.deny_type
    -- ,deny.hour_minute
    -- ,ignore.order_id
    ,ignore.hour_minute
    ,'ignore' as type_deny_ignore
from pool_driver p
left join base_assign1 ignore
    on p.shipper_id = ignore.shipper_id
where ignore.hour_minute not between 11 and 11.5
)
select 
    *
from deny_ignore

where shipper_id in 
(4668791
,21751419
,11905757
,19876569
,21864232
,20288386
,21748690
,16408221
,20722042
,17160388
,8169522
,15516807
,14016183
,7892213
,21721686
,5264715
,22112598
,3363150
,15908702
,10555555
,19238694
,20587546
,20808808
,20793606
,17117340
,19268250
,21749917
,16510055
,21548125
,18869245
,21577693
,20719348
,16889905
,3151190
,19233866
,17191585
,15512607
,8041849
,20458335
,22141787
,18351431
,4750302
,21659610
,15637366
,4474339
,21621969
,22264377
,7069281
,21841750
,21863568
,22227363
,17546816
,21765965
,18272010
,21922806
,17128738
,21959576
,12940176
,20723078
,4378897
,18716081
,18790798
,10166890
,19468340
,4414126
,20344903
,10009964
,22036223
,17195256
,10337254
,22266470
,21997718
,15683336
,18928440
,19076512
,22046732
,14527977
,20743979
,10631332
,7309803
,15639714
,10046358
,16806041
,20126015
,6739133
,20459026
,17602950
,21646781
,12466791
,11905678
,17451654
,22002028
,20658107
,20985766
,20587665
,21809726
,20733045
,22230826
,16848556
,22009204
,21075457
,12215218
,11896850
,13143448
,11686206
,21862870
,6785647
,20233762
,18473987
,4436173
,16302265
,15852507
,19060872
,18521518
,21855280
,4500124
,22222024
,15199648
,15596154
,18512906
,20653811
,11099569
,17813917
,18550331
,21753366
,21722001
,22236066
,10151592
,14748574
,21008330
,14250766
,8031128
,21621906
,16822901
,14623295
,19137630
,18774806
,8558837
,21917535
,21633979
,21655209
,20681280
,21548595
,19173690
,21552898
,17409443
,15224369
,21659506
,19320299
,15853591
,22264511
,16397647
,17020010
,12255554
,20399420
,15522209
,15071025
,17301257
,14436841
,19398989
,21863149
,14680240
,20816719
,19490504
,16366766
,22227610
,21959536
,6093904
,15522887
,22015136
,20490463
,18448800
,21997470
,11802424
,20603960
,4241780
,10708361
,7618890
,21841705
,20676421
,22116175
,22182211
,21533820
,16584212
,22039121
,22200755
,21151066
,22234013
,6984436
,11906476
,15875462
,22214955
,19481460
,21841972
,15829069
,13178086
,21922355
,15595578
,18421936
,20688265
,10108993
,15685430
,7003951
,22182079
,21144911
,20511953
,20222024
,16365973
,21863580
,22047216
,22219392
,8199344
,10403335
,20613581
,20586945
,17107365
,20989258
,20562039
,20804800
,14130812
,22181108
,21722238
,21621829
,20721673
,21444331
,21839378
,12535135
,14617158
,20390619
,21863365
,22227349
,18746329
,14960476
,6622657
,20622966
,16009183
,21975414
,10566446
,15994476
,21477708
,22001016
,20503217
,21851270
,15296092
,20279893
,21851764
,8906949
,20829885
,8014767
,16244649
,22002513
,22227608
,22009397
,21951248
,22112485
,20407437
,12817534
,18845064
,21851193
,22230905
,22187223
,22137169
,19329436
,10808517
,14064907
,10743349
,22088805
,20116719
,18034996
,21552861
,22263629
,21449347
,18196056
,21235475
,17335164
,21997785
,15848660
,18350309
,5200371
,18196383
,7818269
,20407174
,18532098
,4926453
,21659308
,22021379
,20281037
,20860425
,3528913
,20805991
,13064243
,22215673
,22200439
,3398993
,9158430
,17193753
,15440387
,11831194
,20830248
,12478602
,20721776
,17979634
,15446064
,19093329
,21902863
,21208051
,22046675
)