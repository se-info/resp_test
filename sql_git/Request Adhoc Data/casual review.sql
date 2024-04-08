-- (select * from dev_vnfdbi_opsndrivers.shopeefood_vn_food_accountant_driver_order_daily_income_tab


-- order by grass_date desc )



with raw as 
(select
         date(from_unixtime(rp.report_date - 3600)) as report_date
        ,rp.uid
        ,total_completed_order
        ,total_online_seconds/cast(3600 as double) as online_time
        ,total_work_seconds/cast(3600 as double) as working_time 
        ,dense_rank()over(partition by rp.uid order by rp.report_date asc) as rank

from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp

where 1 = 1  
and date(from_unixtime(rp.report_date - 3600)) between date'2022-11-09' and date'2022-11-13'
)

select 
        raw.report_date
       ,raw.uid as shipper_id
       ,raw_v2.report_date as first_order_date 
       ,raw.total_completed_order 
       ,raw.online_time
       ,raw.working_time
       ,coalesce(ic.income,0) as total_income 
       ,coalesce(bonus,0) as daily_bonus 
       ,coalesce(additional_bonus,0) as other_bonus
       ,bl.balance/cast(100 as double) as hold_on_cash_current


from raw         

left join raw raw_v2 on raw_v2.uid = raw.uid and raw_v2.rank = 1 

left join dev_vnfdbi_opsndrivers.shopeefood_vn_food_accountant_driver_order_daily_income_tab ic on ic.driver_id = raw.uid and ic.grass_date = raw.report_date

left join shopeefood.foody_accountant_db__partner_balance_tab__reg_daily_s0_live bl on bl.user_id = raw.uid 


where raw.uid in 
(20762008
,20796677
,40123221
,20755709
,40123477
,40123763
,40123056
,40123173
,40123716
,40123695
,40123666
,40123800
,40124196
,20764856
,20765614
,20771635
,40128318
,40127220
,40126627
,40128514
,40127025
,40127985
,40128686
,40128723
,20821211
,40132326
,40132837
,40133960
,40133327
,40133396
,40133338
,40133789
,40133708
,40133874
,40147150
,40146819
,40146763
,40146390
,20678862
,40147676
,40147695
,40148418
,40147947
,40148077
,40148178
,40147963
,40148308
,40148369
,20833839
,40148687
,40151489
,40152430
,40152506
,40152509
,40152730
,40152879
,40152885
,40153091
,40153174
,40153197
,40153249
,40153484
,40155930
,40156013
,40156374
,40157927
,20833821
,40162722
,40162720
,40128795
,40128892
,40128801
,40128822
,40128828
,40128832
,40128854
,40128862
,40128903
,40128915
,40128931
,40128936
,40129018
,40129023
,40129168
,40129221
,40133561
,40133566
,40133588
,40133626
,40133630
,40133639
,40133655
,40133659
,40133765
,40133679
,40133684
,40133688
,40133690
,40133707
,40147863
,40147876
,40147885
,40147891
,40147901
,40147921
,40148222
,40147731
,40148449
,40148460
,40148487
,40148251
,40148500
,40148505
,20760722
,40148241
,40148516
,40148797
,40148810
,40152640
,20821369
,40152644
,40152653
,40152671
,40153139
,40153152
,40153148
,40153305
,40153314
,40153455
,40153459
,40157760
,40157671
,20854668
,40157845
,40157848
,40157683
,20797535
,40158085
,20832669
,20775818
,40157519
,40158016
,20687006
,40157861
,20797892
,40158026
,40158054
,40158029
,40158034
,40158068
,40158073
,40158077
,40158294
,40158289
,40160938
,40160901
,40160923
,20692095
,40162521
,40162559
,40162590
,40162595
,40162600
,40162606
,40162552
,20809415
,40162691
,40162810
,40162805
,40162794)	
