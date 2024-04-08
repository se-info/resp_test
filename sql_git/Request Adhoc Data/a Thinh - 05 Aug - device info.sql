with raw as 
(select 
        *,row_number()over(partition by uid order by location_create_time desc) as rank_1
         ,row_number()over(partition by uid order by location_create_time asc) as rank_2
         ,date(from_unixtime(location_create_time - 3600)) as location_create_date




from shopeefood.foody_partner_archive_db__shipper_location_log_tab__reg_daily_s0_live

)


select uid,location_create_date,app_version,device_info from raw where (rank_1 = 1 or rank_2 = 1) 

and uid in 

(23079619
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
,18710412)

