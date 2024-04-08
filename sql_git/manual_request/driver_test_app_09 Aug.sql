with raw as 
(select 
        *,row_number()over(partition by uid order by location_create_time desc) as rank_1
         ,row_number()over(partition by uid order by location_create_time asc) as rank_2
         ,date(from_unixtime(location_create_time - 3600)) as location_create_date




from shopeefood.foody_partner_archive_db__shipper_location_log_tab__reg_daily_s0_live

)

,driver_performance as 
(select  
         date(from_unixtime(a.report_date - 3600)) as report_date 
        ,a.uid 
        ,sm.shipper_name 
        ,sm.city_name 
        ,IF(sm.shipper_type_id = 12, 'Hub','Non Hub') as working_type
        ,total_completed_order 
        ,completed_rate*1.00/100 as sla_
        ,total_work_seconds/cast(3600 as double) as working_time
        ,total_online_seconds/cast(3600 as double) as online_time







from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live  a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date ) = date(from_unixtime(a.report_date - 3600))

where 1 = 1 


and date(from_unixtime(report_date - 3600)) between date'2022-07-26' and current_date - interval '1' day
)
select 
         raw.uid
        ,raw.location_create_date
        ,raw.app_version,device_info
        ,total_completed_order 


from raw 

left join driver_performance dp on dp.uid = raw.uid and raw.location_create_date = dp.report_date 


where raw.rank_2 = 1 

and raw.uid in 
(5077915
,3745617
,8169522
,3655781
,1620946
,2996387
,8566908
,9507716
,3534856
,9860279
,3764394
,12026416
,3277567
,12026176
,8551029
,10844400
,2899086
,21667272
,3685935
,1415242
,15119998
,3601361
,10844400
,13136734
,16533298
,12099296
,8618999
,7182579
,11135779
,10715586
,9899244
,14623295
,17137619
,16021629
,6627157
,21998161
,7214050
,21617625
,6592436
,20328639
,22025699
,20475175
,16802140
,17144686
,17868128
,10801391
,22220990
,22732006
,23078924
,12720166
,23097026
,19150251
,17936512
,22100485
,20455751
,6770732
,10265055
,22035047
,17303897
,15268197
,17877009
,4175674
,17470324
,20312684
,8252088
,15521628
,20202010
,22333902
,6683318
,15828853
,17869053
,21738560
,15272964
,16169364
,23081088
,22522974
,22136414
,22188819
,23079619
,18550543
,10705452
,17596854
,19612631
,15277993
,20222288
,20233831
,9399606
,6726535
,20336698
,12668694
,4185385
,23121179
,10266760
,21444331
,12805708
,12162544
,11135646
,23087216
,15641263
,17659977
,12120619
,15272733
,13044802
,16657841
,3793337
,15381150
,10120480
,20732376
,23074808
,14231703
,19372761
,6755070
,23101324
,23083653
,23103257
,23101331
,23091612
,23109122
,21867479
,20852665
,15451826
,10566446
,23101375
,17369696
,16398678
,8090553
,17526701
,8780903
,22181184
,10660282
,18710412
,13146896
,23060515
,7564732
,20249630
,18206885
,18670091
,9353903
,19165327
,17451185
,18921436
,12026206
,18950477
,20328714
,17926433
,23060551
,22413547
,22900891
,21352351
,21236124
,15839955
,7664662
,14626620
,23050394
,23006266
,22927396
,23074061
,23089343
,19388754
,22215119
,20397425
,14304823
,20458413
,20228978
,3686591
,3613325
,14661673
,23117046
,4175818
,22907385
,20681976
,20511396
,4212779
,3643057
,20228987
,20687107
,4626489
,13031181
,5118456
,20035297
,21470104
,19244517
,19244603)

