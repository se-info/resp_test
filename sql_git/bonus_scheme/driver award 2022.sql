with raw as
(select 
        YEAR(from_unixtime(rp.report_date - 3600))*100 + MONTH(from_unixtime(rp.report_date - 3600)) as year_month
       ,date(from_unixtime(rp.report_date - 3600)) as report_date  
       ,rp.uid as shipper_id
       ,sp.shopee_uid 
       ,date(from_unixtime(sp.create_time - 3600)) as onboard_date 
       ,sm.city_name
       ,sm.shipper_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as driver_type
       ,2022 - cast(substr(cast(sp.birth_date as varchar),1,4)  as bigint) as age 
       ,sp.gender
       ,rp.total_delivered_order
       ,rp.total_work_distance/cast(1000 as double) as working_distance 
       ,coalesce(rate.total_5_star,0) as total_5_star
       ,case when sm.shipper_status_code = 1 then 'Working' else 'Others' end as working_status 
    --    ,max_by(date(from_unixtime(rp.report_date - 3600)),rp.uid) as last_active

 



from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp 

LEFT JOIN
(SELECT
date(create_ts) as report_date
,shipper_id
,count(distinct case when shipper_rate = 5 then order_id else null end) as total_5_star
FROM 
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
) 
group by 1,2
) rate ON rp.uid = rate.shipper_id and rate.report_date = date(from_unixtime(rp.report_date - 3600))

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = rp.uid and try_cast(sm.grass_date as date) = date(from_unixtime(rp.report_date - 3600))

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp on sp.uid = rp.uid 
where date(from_unixtime(rp.report_date - 3600)) between date'2022-01-01' and current_date - interval '1' day
order by rp.report_date desc 
)

select 
       raw.shipper_id
      ,raw.shopee_uid 
      ,max_by(raw.shipper_name,raw.report_date) as shipper_name  
      ,max_by(raw.city_name,raw.report_date) as city_name 
      ,max_by(raw.working_status,raw.report_date) as working_status
      ,max_by(raw.age,raw.report_date) as age 
      ,max_by(raw.gender,raw.report_date) as gender 
      ,max_by(raw.driver_type,raw.report_date) as driver_type 
      ,max_by(raw.onboard_date,raw.report_date) as onboard_date
      ,max(report_date) as last_active_date
      ,COALESCE(SUM(case when year_month = 202201 then total_delivered_order else null end),0) as total_jan 
      ,COALESCE(SUM(case when year_month = 202202 then total_delivered_order else null end),0) as total_feb
      ,COALESCE(SUM(case when year_month = 202203 then total_delivered_order else null end),0) as total_mar 
      ,COALESCE(SUM(case when year_month = 202204 then total_delivered_order else null end),0) as total_apr 
      ,COALESCE(SUM(case when year_month = 202205 then total_delivered_order else null end),0) as total_may 
      ,COALESCE(SUM(case when year_month = 202206 then total_delivered_order else null end),0) as total_jun 
      ,COALESCE(SUM(case when year_month = 202207 then total_delivered_order else null end),0) as total_jul 
      ,COALESCE(SUM(case when year_month = 202208 then total_delivered_order else null end),0) as total_aug 
      ,COALESCE(SUM(case when year_month = 202209 then total_delivered_order else null end),0) as total_sep 
      ,COALESCE(SUM(case when year_month = 202210 then total_delivered_order else null end),0) as total_oct 
      ,COALESCE(SUM(case when year_month = 202211 then total_delivered_order else null end),0) as total_nov 
      ,COALESCE(SUM(case when year_month = 202212 then total_delivered_order else null end),0) as total_dec 
      ,COALESCE(SUM(total_delivered_order),0) as total_order_2022
      ,COALESCE(SUM(working_distance),0) as total_working_distance_2022
      ,COALESCE(SUM(total_5_star),0) as total_5_star_2022
      ,COUNT(DISTINCT report_date) as total_working_day_2022


from raw 



where 1 = 1 

group by 1,2 
;
-- check violation
with raw as 
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
                      
                FROM shopeefood.foody_internal_db__hr_tick_tab__reg_daily_s0_live ht 
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_label_tab__reg_daily_s0_live htl on htl.tick_id = ht.id
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_user_tab__reg_daily_s0_live htu on htu.tick_id = ht.id
                LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = htu.uid and sm.grass_date = 'current'
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_order_tab__reg_daily_s0_live tot ON (ht.id = tot.tick_id)
                WHERE 1=1
                -- and ht.incharge_team = 4
                and date(from_unixtime(ht.create_time - 60*60)) between date'2022-01-01' and current_date - interval '1' day
)

select 
        shipper_id 
       ,max_by(shipper_name,created_timestamp) as shipper_name
       ,count(distinct ticket_id) as total_case 
       ,map_agg(ticket_id,resolution) as tick_id_x_resolution 



from raw 
where resolution in ('FINE_SHIPPER','FIRED_SHIPPER')
-- where resolution like '%FIRED%'
and shipper_id in 
(3745617,
12438438,
20312549,
9257204,
17480940,
16397647,
21722008,
6847691,
13115005,
20817234,
15717733,
20733274,
16365777,
16584212,
15618653,
10092706,
14950126,
20355574,
18607169,
18780862,
15126547,
21152065,
4185385,
19044687,
16698356,
14617158,
3690547,
21465756,
18780814,
17875997,
19329325,
20241153,
20755737,
19884185,
18774389,
14523272,
20446322,
15595578,
18283210,
20749435,
8452306,
14373469,
20400807,
19885035,
18196413,
16449688,
17191636,
19124845,
18848342,
16625016,
8997994,
19054829,
16365973,
21081881,
16587202,
17049823,
20288149,
17205979,
20328943,
17097799,
20808274,
5138323,
5319319,
6928718,
10800877,
11837878,
15129359,
20481867,
7908786,
18844889,
4154308,
4613279,
10631733,
11989554,
14474836,
3604805,
6005140,
8251730,
9394241,
10324197,
11721014,
11686279,
11747357,
11846121,
13143517,
2703129,
7158188,
8063256,
9975701,
10808517,
13183202,
14374723,
15129349,
15608195,
16251108,
16523711,
20008578,
5845798,
6947431,
7740857,
8313047,
11817449,
12100779,
12263591,
12470449,
12440321,
12396032,
12453118,
12926099,
13023653,
14308019,
14604181,
14608004,
14623268,
15129299,
16224117,
18057180,
40066054,
684821,
858051,
1280992,
1280998,
1125774,
1280987,
1413231,
1415242,
1418220,
1417339,
1420783,
1418132,
94900,
1425088,
1422718,
1447682,
1913009,
1146567,
1913968,
1928990,
1941919,
1924707,
1990348,
1990321,
1950331,
1976202,
2014314,
2012588,
2033431,
2047660,
2029064,
2095635,
2115296,
2355648,
2068120,
2432427,
2429507,
2414553,
2414001,
2407757,
2438679,
2418423,
2475797,
2475900,
2481554,
2475882,
2491465,
2551796,
2593361,
2569019,
2599695,
2599357,
2568161,
2576025,
2587515,
2559680,
2551300,
2578406,
2593310,
7214050,
20749345,
18011691,
20935540,
20610146,
4414126,
11135779,
20242911,
20821290,
1280998,
10391947,
11905256,
11814708,
12004880,
14002188,
4758167,
21447538,
19167283,
9699742,
3745617,
15380584,
15392293,
6785647,
10265889,
17137619,
4585455,
7994331,
12162544,
20852979,
10156554,
20794352,
2568161,
10840848,
18403116,
20698350,
19058552,
10643566,
20233678,
20400791,
21008230,
20312549,
16848647,
9257204,
6739133,
17480940,
19161849,
21470019,
16445107,
16378588,
20797967,
16397647,
15706355,
20309574,
3685935,
20729865,
21862870,
20761163,
16224892,
20285587,
11912932,
21735277,
8190062,
20681280,
19044749,
14064907,
17194119,
20159008,
13186751,
20610088,
17546231,
20688265,
20561950,
9497986,
6475538,
18746449,
20457631,
20309577,
11135646,
6656063,
15539229,
15629782,
19092618,
16408221,
20586945,
17329954,
6984436,
10360646,
20601232,
16833952,
12964834,
21722008,
21617625,
10844400,
11686206,
15444487,
9899244,
7214203,
19705516,
20222020,
18360300,
18395570,
16021113,
20399420,
15947793,
11820423,
20676389,
4526776,
20088833,
21642291,
10665386,
19537907,
20806420,
6627157,
6783422,
18227058,
20471947,
8026573,
15462864,
3084715,
21713583,
20584824,
3534875,
6848650,
20390619,
19525922,
15625339,
5077915,
20860425,
8277535,
16889905,
17204850,
20126015,
8026134,
19141206,
4473742,
13143448,
20301830,
20817234,
15697059,
20310870,
15717733,
18475148,
7994379,
18521518,
12706635,
15924889,
12633447,
12095391,
14451302,
13146896,
21070211,
17157881,
15557023,
14329411,
15561484,
14704515,
12763201,
17526725,
12438438,
15288927,
11848213,
9938930,
11893065,
19689010,
3714133,
19399390,
11852301,
12035293,
16234763,
13044502,
20464295,
16169364,
20280770,
17550776,
18985272,
16378556,
12255554,
12035340,
6570205,
4384543,
16373521,
19690551,
6847691,
15283741,
13174544,
4642731,
17748461,
5011824,
6785859,
20229116,
20372388,
18140544,
20286293,
10260732,
6885755,
12035362,
16365141,
16628821,
20525949,
12518493,
15285095,
14810376,
9899780,
19172966,
10495772,
12565436,
15139575,
19162740,
19149582,
8204974,
11821184,
12298065,
14760544,
20658198,
10198010,
16607489,
6935258,
5849662,
3158005,
18328162,
8774442,
20516569,
16826992,
18483339,
14471888,
8587447,
15785072,
20249630,
13115005,
12000058,
17698444,
12804088,
21293496,
17522695,
10232688,
16815364,
17477396,
17374971,
19173502,
12187915,
16556214,
17479965,
16109982,
15837040,
14796710,
5263414,
17823247,
18669804,
12217021,
12731743,
8879511,
12666099,
13178503,
18207942,
20516603,
12470449,
14130715,
20233436,
15629375,
8024760,
12095559,
3713749,
16805721,
3775060,
20294506,
18216154,
18043639,
7897998,
15618653,
3912021,
10088593,
12251470,
20227411,
13099564,
18950477,
18848136,
7777655,
10717302,
6834512,
17107517,
14002209,
17201624,
17404315,
16477902,
12173026,
4816923,
11993332,
15358936,
21201374,
17134987,
20258568,
16165138,
12099271,
15556241,
18718635,
14950126,
14325098,
11136063,
10719663,
13146896,
7214050,
20749345,
18011691,
20935540,
20610146,
21070211,
4414126,
11135779,
20242911,
20821290,
1280998,
10391947,
11905256,
11814708,
12004880,
14002188,
4758167,
17157881,
15557023,
21447538,
19167283,
9699742,
3745617,
15380584,
15392293,
6785647,
10265889,
17137619,
4585455,
14329411,
7994331,
12162544,
20852979,
10156554,
20794352,
15561484,
2568161,
10840848,
18403116,
14704515,
12763201,
20698350,
19058552,
10643566,
20233678,
20400791,
17526725,
21008230,
12438438,
20312549,
16848647,
9257204,
6739133,
17480940,
19161849,
21470019,
16445107,
15288927,
16378588,
20797967,
16397647,
15706355,
11848213,
20309574,
3685935,
20729865,
21862870,
20761163,
9938930,
16224892,
11893065,
20285587,
11912932,
21735277,
8190062,
20681280,
19044749,
14064907,
17194119,
20159008,
13186751,
19689010,
3714133,
20610088,
17546231,
20688265,
19399390,
20561950,
11852301,
12035293,
16234763,
13044502,
20464295,
9497986,
6475538,
16169364,
18746449,
20457631,
20309577,
11135646,
6656063,
20280770,
15539229,
17550776,
18985272,
16378556,
15629782,
12255554,
12035340,
19092618,
16408221,
6570205,
20586945,
17329954,
4384543,
6984436,
10360646,
20601232,
16833952,
12964834,
16373521,
21722008,
21617625,
10844400,
11686206,
19690551,
15444487,
6847691,
9899244,
15283741,
7214203,
13174544,
19705516,
20222020,
18360300,
4642731,
17748461,
18395570,
16021113,
20399420,
5011824,
15947793,
6785859,
11820423,
20676389,
4526776,
20088833,
20229116,
21642291,
20372388,
18140544,
20286293,
10260732,
6885755,
12035362,
16365141,
10665386,
19537907,
16628821,
20806420,
6627157,
6783422,
20525949,
18227058,
12518493,
20471947,
15285095,
14810376,
8026573,
15462864,
9899780,
3084715,
19172966,
10495772,
12565436,
21713583,
15139575,
19162740,
19149582,
8204974,
11821184,
20584824,
3534875,
6848650,
12298065,
14760544,
20390619,
20658198,
19525922,
10198010,
15625339,
16607489,
6935258,
5849662,
3158005,
18328162,
5077915,
8774442,
20516569,
20860425,
16826992,
18483339,
14471888,
8587447,
8277535,
16889905,
15785072,
20249630,
17204850,
20126015,
8026134,
13115005,
19141206,
4473742,
13143448,
12000058,
17698444,
12804088,
21293496,
17522695,
10232688,
20301830,
20817234,
15697059,
16815364,
20310870,
17477396,
17374971,
15717733,
18475148,
7994379,
18521518,
12706635,
19173502,
15924889,
12187915,
12633447,
16556214,
12095391,
17479965,
14451302,
20733274,
19316046,
14450235,
20344317,
16109982,
13023745,
18413125,
10754299,
15837040,
14796710,
19305403,
16365777,
10189691,
20483489,
5263414,
17823247,
18669804,
14436841,
4512472,
12217021,
10628808,
12731743,
14100457,
11788607,
8879511,
12666099,
20160224,
13178503,
18207942,
12291909,
16584212,
20516603,
20936413,
17077156,
12470449,
15671717,
14130715,
20233436,
20821615,
15629375,
8024760,
17191585,
10051279,
12095559,
9661765,
3713749,
15071025,
16805721,
3775060,
20294506,
14286262,
20390725,
10631332,
18918351,
18216154,
21804618,
12090079,
19070413,
20796502,
18043639,
7897998,
20595917,
15618653,
3912021,
10088593,
12251470,
17438712,
20227411,
13099564,
18950477,
13177363,
10092706,
19468340,
18848136,
7777655,
20658056,
10717302,
19305113,
19329436,
6834512,
17107517,
14002209,
20744167,
17201624,
19882336,
7069131,
17404315,
16477902,
12173026,
19372715,
4816923,
11993332,
15358936,
21201374,
17134987,
20258568,
16165138,
12099271,
20344466,
15556241,
6762629,
22021822,
18718635,
11953901,
21561649,
14950126,
10392551,
14325098,
11136063,
15119971,
7109046,
10719663,
17291861,
15433504,
21172400,
16395633,
20601127,
12483235,
20586976,
14544322,
20355574,
20336555,
4474364,
20122343,
6498574,
18342508,
18607169,
21553273,
15358936,
18011691,
19269336,
12817546,
20525949,
7214050,
20749345,
4758167,
18207942,
3745617,
12004880,
11814708,
20935540,
9699742,
20852979,
20794352,
4500124,
10156554,
11135779,
3084715,
1280998,
19329436,
4414126,
21070211,
13146896,
12714201,
20821290,
17546231,
17137619,
16397647,
20159008,
20242911,
14002188,
11905256,
20516569,
20610146,
10643566,
19161849,
20312549,
20233678,
21447538,
21008230,
19058552,
16848647,
7994331,
11686206,
15392293,
10704871,
21862870,
6785647,
4585455,
2568161,
20698350,
13143517,
8026134,
21470019,
20285587,
17550603,
20372388,
18216154,
20294506,
15629782,
10391947,
13143448,
20309577,
9363891,
6656063,
3685935,
21029630,
16833952,
20729865,
20088833,
21617625,
6739133,
9497986,
17470952,
16223755,
16378588,
14329411,
13186751,
21201374,
10840848,
16408221,
16373521,
15119971,
9257204,
8190062,
20601232,
20286293,
12763201,
20400791,
10844400,
15444487,
18360300,
15462689,
6847691,
19689010,
17194119,
12964834,
14889143,
21735277,
20464295,
6984436,
20483489,
16889905,
15380584,
20457631,
20806420,
12483235,
16224892,
20399420,
17480940,
20309574,
9899244,
20688265,
20455277,
15706355,
11820423,
4512472,
12255554,
21722008,
19705516,
19537907,
15380653,
15947793,
18742857,
20761163,
18395570,
16365141,
17204850,
19620302,
18746449,
20229116,
12035340,
19167283,
16445107,
18939092,
11893065,
12076760,
13183994,
14069462,
12706635,
20681280,
17526725,
21634564,
10051279,
20797967,
6783422,
8026573,
20584824)

group by 1