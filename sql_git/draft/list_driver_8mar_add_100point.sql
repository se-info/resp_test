/*
Detail scheme: https://gofast.vn/tin-tuc/hcm-hn-mo-ung-dung-lien-tay-nhan-uu-dai-500k/
*/




with order_detail AS 
(SELECT
    dot.uid as shipper_id
    ,dot.ref_order_id as order_id
    ,dot.ref_order_code as order_code
    ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
    ,dot.ref_order_category
    ,case when dot.ref_order_category = 0 then 'order_delivery'
        when dot.ref_order_category = 3 then 'now_moto'
        when dot.ref_order_category = 4 then 'now_ship'
        when dot.ref_order_category = 5 then 'now_ship'
        when dot.ref_order_category = 6 then 'now_ship_shopee'
        when dot.ref_order_category = 7 then 'now_ship_sameday'
        else null end source
    ,dot.ref_order_status
    ,dot.order_status
    ,case when dot.order_status = 1 then 'Pending'
        when dot.order_status in (100,101,102) then 'Assigning'
        when dot.order_status in (200,201,202,203,204) then 'Processing'
        when dot.order_status in (300,301) then 'Error'
        when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
        else null end as order_status_group

    ,dot.is_asap

    ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
    ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date

    ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
WHERE 1=1 
AND dot.order_status = 400 -- delivered orders 
AND case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end <= date '2022-03-06' -- report_date from 06-03 lookback
)
,performance_driver as
(
    SELECT distinct
    dot.uid as shipper_id
    ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
    else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
    -- ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
WHERE 1=1 
AND dot.order_status = 400 -- delivered orders 
AND (
    case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
    else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end = date '2022-03-07'
    or
    (case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
    else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end = date '2022-03-08' 
        and 
    -- remove shipper_id da add 500 point bacth_1
    dot.uid not in (19304716,4793388,16657720,21081688,17823826,17162348,18791271,15930451,17206983,15264586,20391170,20661722,7618946,20446777,11793119,17161720,11897308,17097146,20661724,19070365,20459207,15263799,20742635,15139381,17933649,20285686,8314979,11847421,9262365,19647024,20202373,19176821,17118957,10083491,18258363,17115194,15874561,12011171,10588103,19161123,17482302,18209315,8024709,17097896,4208487,3152956,17980078,17508354,16597877,19154256,16607402,19244653,11892944,19537323,11748900,20393414,17067202,4543747,20796901,18196590,16976242,20662264,20678408,15235038,20450778,15598305,16409111,19625022,20467067,17892134,17919432,18547019,16816381,17890283,19561968,11705518,12083659,17550789,12165763,18463294,16362959,17893010,19643524,13032826,16181135,20491911,17046537,6768448,20331220,20600801,15059835,15714017,20220370,20720993,8266926,6798031,16682033,17077498,20278665,12859042,18395366,17876197,20491010,20466874,21094418,10842452,20730898,18878946,12427361,17881458,16315933,14742414,17884414,13025214,16362930,15440369,19136228,20667769,20193955,14450252,17194880,8137139,18799166,18258791,19486829,18395621,21200555,10277892,17520985,20456061,20755759,11848461,17546477,16018789,17195560,13208786,20057774,11686647,20338761,19221824,20226608,17529948,17522157,20407288,19602039,15836258,7052515,14626794,10416683,19476870,17077550,20808723,13142779,20187820,19498453,4273214,14304632,20621484,21533837,16603311,20328775,16522977,16512632,17889085,19379159,21064091,17461836,18856258,16680409,20652673,16179626,20587212,17096713,17096703,10598806,16533382,15512519,18747448,20056467,9965488,16305660,17869360,16593367,8551312,18290941,17088046,11814350,11715197,8949430,10092361,12039028,18410911,18322392,17195445,15716844,17196334,17742412,14390400,16334166,17195759,20391217,12920183,11716178,18532141,8450907,18774446,20235965,21470601,11102238,15625624,20404506,12920508,19465135,5101111,18480879,4067455,19501610,10709601,20278576,20291300,19562189,20035559,18584937,8008356,19315881,19248672,18609273,10577853,16555039,14123447,17482046,18947611,19490493,20355584,20822133,12434055,21093703,15793685,17928110,10400166,20175686,17145076,13248006,21160081,21070513,19221318,11832016,21026361,12661520,17015757,21062677,18333920,17462247,21009421,19476099,20459817,8517241,19379555,16294796,6000259,20407478,21307403,16752118,20653809,12120904,18609308,12071977,16805649,21460386,11742489,16479802,20652189,16535105,10565094,18918989,11141504,9785574,17919918,12946006,18644416,19150822,18872358,10434487,9876077,19445557,14437965,7761129,19315871,20353421,20287726,15782844,6583472,16480294,20309723,21145430,16261359,3685869,18463249,19458642,17191051,18662697,9271879,20332413,6966814,20425196,18923397,19255608,17521392,18581131,18589546,18529408,21219462,10041167,20294545,19526306,20506388,21465570,16510052,16036983,20475176,19679510,17451497,19498132,17201862,11715956,17520232,20474712,17438576,20336560,10401581,20764434,17522201,14617050,17468256,16887385)
    )
    or 
    case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
    else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end between date '2022-03-09' and date '2022-03-13'
    )
)

select distinct
    pd.report_date
    ,pd.shipper_id
    ,filter.hub_type
    ,case when pd.shipper_id is not null then 1 else 0 end as is_have_order_completed
    ,filter.city_id
    ,case when pd.report_date = date '2022-03-08' then 500 else 100 end as point_add
    ,max(o.report_date) as last_active_date
from performance_driver pd
left join order_detail o
    on pd.shipper_id = o.shipper_id
left join 
    (select 
        shipper_id
        ,case when shipper_type_id = 12 then 'Hub' else 'Non-Hub' end as hub_type
        ,city_id as city_id
        ,grass_date
    from shopeefood.foody_mart__profile_shipper_master
    where grass_region = 'VN'
    -- and try_cast(grass_date as date) = date '2022-03-06' -- replace ngay check shipper_type
    and shipper_type_id <> 12 -- NonHUB
    and city_id in (218,217) -- HN + HCM
        ) as filter
    on pd.shipper_id = filter.shipper_id and pd.report_date = try_cast(filter.grass_date as date)

where filter.hub_type = 'Non-Hub'
and filter.city_id in (217,218)

group by 1,2,3,4,5,6
having max(o.report_date) <= date '2022-03-06' - interval '15' day
