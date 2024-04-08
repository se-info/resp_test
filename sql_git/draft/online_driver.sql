

SELECT base1.create_date
,base1.created_year_week
,concat(concat(substr(cast(base1.created_year_week as VARCHAR),1,4),'-W'),substr(cast(base1.created_year_week as VARCHAR),5,2)) as created_year_week_text
,base1.created_year_month
,base1.city_name
,base1.city_group

-- tier
,base1.current_driver_tier
--,base1.new_driver_tier

--driver
,count(distinct case when base1.total_online_time > 0 then base1.shipper_id else null end) total_driver_online
,count(distinct case when base1.total_working_time > 0 then base1.shipper_id else null end) total_driver_work


-----by timeslot

,count(distinct case when base1.h0_online_time > 0 then base1.shipper_id else null end) h00_total_driver_online
,count(distinct case when base1.h1_online_time > 0 then base1.shipper_id else null end) h01_total_driver_online
,count(distinct case when base1.h2_online_time > 0 then base1.shipper_id else null end) h02_total_driver_online
,count(distinct case when base1.h3_online_time > 0 then base1.shipper_id else null end) h03_total_driver_online
,count(distinct case when base1.h4_online_time > 0 then base1.shipper_id else null end) h04_total_driver_online
,count(distinct case when base1.h5_online_time > 0 then base1.shipper_id else null end) h05_total_driver_online
,count(distinct case when base1.h6_online_time > 0 then base1.shipper_id else null end) h06_total_driver_online
,count(distinct case when base1.h7_online_time > 0 then base1.shipper_id else null end) h07_total_driver_online
,count(distinct case when base1.h8_online_time > 0 then base1.shipper_id else null end) h08_total_driver_online
,count(distinct case when base1.h9_online_time > 0 then base1.shipper_id else null end) h09_total_driver_online
,count(distinct case when base1.h10_online_time > 0 then base1.shipper_id else null end) h10_total_driver_online
,count(distinct case when base1.h11_online_time > 0 then base1.shipper_id else null end) h11_total_driver_online
,count(distinct case when base1.h12_online_time > 0 then base1.shipper_id else null end) h12_total_driver_online
,count(distinct case when base1.h13_online_time > 0 then base1.shipper_id else null end) h13_total_driver_online
,count(distinct case when base1.h14_online_time > 0 then base1.shipper_id else null end) h14_total_driver_online
,count(distinct case when base1.h15_online_time > 0 then base1.shipper_id else null end) h15_total_driver_online
,count(distinct case when base1.h16_online_time > 0 then base1.shipper_id else null end) h16_total_driver_online
,count(distinct case when base1.h17_online_time > 0 then base1.shipper_id else null end) h17_total_driver_online
,count(distinct case when base1.h18_online_time > 0 then base1.shipper_id else null end) h18_total_driver_online
,count(distinct case when base1.h19_online_time > 0 then base1.shipper_id else null end) h19_total_driver_online
,count(distinct case when base1.h20_online_time > 0 then base1.shipper_id else null end) h20_total_driver_online
,count(distinct case when base1.h21_online_time > 0 then base1.shipper_id else null end) h21_total_driver_online
,count(distinct case when base1.h22_online_time > 0 then base1.shipper_id else null end) h22_total_driver_online
,count(distinct case when base1.h23_online_time > 0 then base1.shipper_id else null end) h23_total_driver_online

,sum(base1.total_online_time) total_online_time

,sum(h0_online_time) h00_online_time
,sum(h1_online_time) h01_online_time
,sum(h2_online_time) h02_online_time
,sum(h3_online_time) h03_online_time
,sum(h4_online_time) h04_online_time
,sum(h5_online_time) h05_online_time
,sum(h6_online_time) h06_online_time
,sum(h7_online_time) h07_online_time
,sum(h8_online_time) h08_online_time
,sum(h9_online_time) h09_online_time
,sum(h10_online_time) h10_online_time
,sum(h11_online_time) h11_online_time
,sum(h12_online_time) h12_online_time
,sum(h13_online_time) h13_online_time
,sum(h14_online_time) h14_online_time
,sum(h15_online_time) h15_online_time
,sum(h16_online_time) h16_online_time
,sum(h17_online_time) h17_online_time
,sum(h18_online_time) h18_online_time
,sum(h19_online_time) h19_online_time
,sum(h20_online_time) h20_online_time
,sum(h21_online_time) h21_online_time
,sum(h22_online_time) h22_online_time
,sum(h23_online_time) h23_online_time

---working
,count(distinct case when base1.h0_working_time > 0 then base1.shipper_id else null end) h00_total_driver_working
,count(distinct case when base1.h1_working_time > 0 then base1.shipper_id else null end) h01_total_driver_working
,count(distinct case when base1.h2_working_time > 0 then base1.shipper_id else null end) h02_total_driver_working
,count(distinct case when base1.h3_working_time > 0 then base1.shipper_id else null end) h03_total_driver_working
,count(distinct case when base1.h4_working_time > 0 then base1.shipper_id else null end) h04_total_driver_working
,count(distinct case when base1.h5_working_time > 0 then base1.shipper_id else null end) h05_total_driver_working
,count(distinct case when base1.h6_working_time > 0 then base1.shipper_id else null end) h06_total_driver_working
,count(distinct case when base1.h7_working_time > 0 then base1.shipper_id else null end) h07_total_driver_working
,count(distinct case when base1.h8_working_time > 0 then base1.shipper_id else null end) h08_total_driver_working
,count(distinct case when base1.h9_working_time > 0 then base1.shipper_id else null end) h09_total_driver_working
,count(distinct case when base1.h10_working_time > 0 then base1.shipper_id else null end) h10_total_driver_working
,count(distinct case when base1.h11_working_time > 0 then base1.shipper_id else null end) h11_total_driver_working
,count(distinct case when base1.h12_working_time > 0 then base1.shipper_id else null end) h12_total_driver_working
,count(distinct case when base1.h13_working_time > 0 then base1.shipper_id else null end) h13_total_driver_working
,count(distinct case when base1.h14_working_time > 0 then base1.shipper_id else null end) h14_total_driver_working
,count(distinct case when base1.h15_working_time > 0 then base1.shipper_id else null end) h15_total_driver_working
,count(distinct case when base1.h16_working_time > 0 then base1.shipper_id else null end) h16_total_driver_working
,count(distinct case when base1.h17_working_time > 0 then base1.shipper_id else null end) h17_total_driver_working
,count(distinct case when base1.h18_working_time > 0 then base1.shipper_id else null end) h18_total_driver_working
,count(distinct case when base1.h19_working_time > 0 then base1.shipper_id else null end) h19_total_driver_working
,count(distinct case when base1.h20_working_time > 0 then base1.shipper_id else null end) h20_total_driver_working
,count(distinct case when base1.h21_working_time > 0 then base1.shipper_id else null end) h21_total_driver_working
,count(distinct case when base1.h22_working_time > 0 then base1.shipper_id else null end) h22_total_driver_working
,count(distinct case when base1.h23_working_time > 0 then base1.shipper_id else null end) h23_total_driver_working

,sum(base1.total_working_time) total_working_time

,sum(h0_working_time) h00_working_time
,sum(h1_working_time) h01_working_time
,sum(h2_working_time) h02_working_time
,sum(h3_working_time) h03_working_time
,sum(h4_working_time) h04_working_time
,sum(h5_working_time) h05_working_time
,sum(h6_working_time) h06_working_time
,sum(h7_working_time) h07_working_time
,sum(h8_working_time) h08_working_time
,sum(h9_working_time) h09_working_time
,sum(h10_working_time) h10_working_time
,sum(h11_working_time) h11_working_time
,sum(h12_working_time) h12_working_time
,sum(h13_working_time) h13_working_time
,sum(h14_working_time) h14_working_time
,sum(h15_working_time) h15_working_time
,sum(h16_working_time) h16_working_time
,sum(h17_working_time) h17_working_time
,sum(h18_working_time) h18_working_time
,sum(h19_working_time) h19_working_time
,sum(h20_working_time) h20_working_time
,sum(h21_working_time) h21_working_time
,sum(h22_working_time) h22_working_time
,sum(h23_working_time) h23_working_time






from
(SELECT base.shipper_id
,base.create_date
,format_datetime(base.create_date,'EE') as created_day_of_week
,case when base.create_date between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when base.create_date between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    else YEAR(base.create_date)*100 + WEEK(base.create_date) end as created_year_week
,concat(cast(YEAR(base.create_date) as VARCHAR),'-','(',date_format(base.create_date,'%m'),')',date_format(base.create_date,'%b')) as created_year_month     

-- original
,base.check_in_time_original
,base.check_out_time_original
,base.order_start_time_original
,base.order_end_time_original

-- actual use
,base.actual_start_time_online
,base.actual_end_time_online
,base.actual_start_time_work
,base.actual_end_time_work
-- peak

-- location
,coalesce(loc.city_name,loc_extra.city_name) as city_name
,coalesce(loc.city_group,loc_extra.city_group) as city_group

-- tier
,case when bonus.current_driver_tier in ('T1','T2','T3','T4','T5') then bonus.current_driver_tier else 'Others' end as current_driver_tier
--,case when bonus.new_driver_tier in ('T1','T2','T3','T4','T5') then bonus.new_driver_tier else 'Others' end as new_driver_tier

-- total
,date_diff('second',base.actual_start_time_online,base.actual_end_time_online)*1.0000/(60*60) as total_online_time
,date_diff('second',base.actual_start_time_work,base.actual_end_time_work)*1.0000/(60*60) as total_working_time

-----------------------------------
--by timeslot

,case when base.actual_end_time_online < base.h0_start then 0
    when base.actual_start_time_online > base.h0_end then 0
    else date_diff('second',   greatest(base.h0_start,base.actual_start_time_online)   ,   least(base.h0_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h0_online_time

,case when base.actual_end_time_online < base.h1_start then 0
    when base.actual_start_time_online > base.h1_end then 0
    else date_diff('second',   greatest(base.h1_start,base.actual_start_time_online)   ,   least(base.h1_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h1_online_time
    
,case when base.actual_end_time_online < base.h2_start then 0
    when base.actual_start_time_online > base.h2_end then 0
    else date_diff('second',   greatest(base.h2_start,base.actual_start_time_online)   ,   least(base.h2_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h2_online_time
    
,case when base.actual_end_time_online < base.h3_start then 0
    when base.actual_start_time_online > base.h3_end then 0
    else date_diff('second',   greatest(base.h3_start,base.actual_start_time_online)   ,   least(base.h3_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h3_online_time

,case when base.actual_end_time_online < base.h4_start then 0
    when base.actual_start_time_online > base.h4_end then 0
    else date_diff('second',   greatest(base.h4_start,base.actual_start_time_online)   ,   least(base.h4_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h4_online_time
    
,case when base.actual_end_time_online < base.h5_start then 0
    when base.actual_start_time_online > base.h5_end then 0
    else date_diff('second',   greatest(base.h5_start,base.actual_start_time_online)   ,   least(base.h5_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h5_online_time
    
,case when base.actual_end_time_online < base.h6_start then 0
    when base.actual_start_time_online > base.h6_end then 0
    else date_diff('second',   greatest(base.h6_start,base.actual_start_time_online)   ,   least(base.h6_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h6_online_time
    
,case when base.actual_end_time_online < base.h7_start then 0
    when base.actual_start_time_online > base.h7_end then 0
    else date_diff('second',   greatest(base.h7_start,base.actual_start_time_online)   ,   least(base.h7_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h7_online_time
    
,case when base.actual_end_time_online < base.h8_start then 0
    when base.actual_start_time_online > base.h8_end then 0
    else date_diff('second',   greatest(base.h8_start,base.actual_start_time_online)   ,   least(base.h8_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h8_online_time
    
,case when base.actual_end_time_online < base.h9_start then 0
    when base.actual_start_time_online > base.h9_end then 0
    else date_diff('second',   greatest(base.h9_start,base.actual_start_time_online)   ,   least(base.h9_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h9_online_time
    
,case when base.actual_end_time_online < base.h10_start then 0
    when base.actual_start_time_online > base.h10_end then 0
    else date_diff('second',   greatest(base.h10_start,base.actual_start_time_online)   ,   least(base.h10_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h10_online_time
    
,case when base.actual_end_time_online < base.h11_start then 0
    when base.actual_start_time_online > base.h11_end then 0
    else date_diff('second',   greatest(base.h11_start,base.actual_start_time_online)   ,   least(base.h11_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h11_online_time
    
,case when base.actual_end_time_online < base.h12_start then 0
    when base.actual_start_time_online > base.h12_end then 0
    else date_diff('second',   greatest(base.h12_start,base.actual_start_time_online)   ,   least(base.h12_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h12_online_time
    
,case when base.actual_end_time_online < base.h13_start then 0
    when base.actual_start_time_online > base.h13_end then 0
    else date_diff('second',   greatest(base.h13_start,base.actual_start_time_online)   ,   least(base.h13_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h13_online_time
       
,case when base.actual_end_time_online < base.h14_start then 0
    when base.actual_start_time_online > base.h14_end then 0
    else date_diff('second',   greatest(base.h14_start,base.actual_start_time_online)   ,   least(base.h14_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h14_online_time

,case when base.actual_end_time_online < base.h15_start then 0
    when base.actual_start_time_online > base.h15_end then 0
    else date_diff('second',   greatest(base.h15_start,base.actual_start_time_online)   ,   least(base.h15_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h15_online_time
 
,case when base.actual_end_time_online < base.h16_start then 0
    when base.actual_start_time_online > base.h16_end then 0
    else date_diff('second',   greatest(base.h16_start,base.actual_start_time_online)   ,   least(base.h16_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h16_online_time
 
,case when base.actual_end_time_online < base.h17_start then 0
    when base.actual_start_time_online > base.h17_end then 0
    else date_diff('second',   greatest(base.h17_start,base.actual_start_time_online)   ,   least(base.h17_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h17_online_time
 
,case when base.actual_end_time_online < base.h18_start then 0
    when base.actual_start_time_online > base.h18_end then 0
    else date_diff('second',   greatest(base.h18_start,base.actual_start_time_online)   ,   least(base.h18_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h18_online_time
 
,case when base.actual_end_time_online < base.h19_start then 0
    when base.actual_start_time_online > base.h19_end then 0
    else date_diff('second',   greatest(base.h19_start,base.actual_start_time_online)   ,   least(base.h19_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h19_online_time
 
,case when base.actual_end_time_online < base.h20_start then 0
    when base.actual_start_time_online > base.h20_end then 0
    else date_diff('second',   greatest(base.h20_start,base.actual_start_time_online)   ,   least(base.h20_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h20_online_time
 
,case when base.actual_end_time_online < base.h21_start then 0
    when base.actual_start_time_online > base.h21_end then 0
    else date_diff('second',   greatest(base.h21_start,base.actual_start_time_online)   ,   least(base.h21_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h21_online_time

,case when base.actual_end_time_online < base.h22_start then 0
    when base.actual_start_time_online > base.h22_end then 0
    else date_diff('second',   greatest(base.h22_start,base.actual_start_time_online)   ,   least(base.h22_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h22_online_time

,case when base.actual_end_time_online < base.h23_start then 0
    when base.actual_start_time_online > base.h23_end then 0
    else date_diff('second',   greatest(base.h23_start,base.actual_start_time_online)   ,   least(base.h23_end,base.actual_end_time_online)   )*1.0000/(60*60)
    end as h23_online_time

--working driver
 ,case when base.actual_end_time_work < base.h0_start then 0
    when base.actual_start_time_work > base.h0_end then 0
    else date_diff('second',   greatest(base.h0_start,base.actual_start_time_work)   ,   least(base.h0_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h0_working_time   
 ,case when base.actual_end_time_work < base.h1_start then 0
    when base.actual_start_time_work > base.h1_end then 0
    else date_diff('second',   greatest(base.h1_start,base.actual_start_time_work)   ,   least(base.h1_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h1_working_time   
 ,case when base.actual_end_time_work < base.h2_start then 0
    when base.actual_start_time_work > base.h2_end then 0
    else date_diff('second',   greatest(base.h2_start,base.actual_start_time_work)   ,   least(base.h2_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h2_working_time   
 ,case when base.actual_end_time_work < base.h3_start then 0
    when base.actual_start_time_work > base.h3_end then 0
    else date_diff('second',   greatest(base.h3_start,base.actual_start_time_work)   ,   least(base.h3_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h3_working_time   
 ,case when base.actual_end_time_work < base.h4_start then 0
    when base.actual_start_time_work > base.h4_end then 0
    else date_diff('second',   greatest(base.h4_start,base.actual_start_time_work)   ,   least(base.h4_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h4_working_time   
 ,case when base.actual_end_time_work < base.h5_start then 0
    when base.actual_start_time_work > base.h5_end then 0
    else date_diff('second',   greatest(base.h5_start,base.actual_start_time_work)   ,   least(base.h5_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h5_working_time   
 ,case when base.actual_end_time_work < base.h6_start then 0
    when base.actual_start_time_work > base.h6_end then 0
    else date_diff('second',   greatest(base.h6_start,base.actual_start_time_work)   ,   least(base.h6_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h6_working_time   
 ,case when base.actual_end_time_work < base.h7_start then 0
    when base.actual_start_time_work > base.h7_end then 0
    else date_diff('second',   greatest(base.h7_start,base.actual_start_time_work)   ,   least(base.h7_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h7_working_time       
 ,case when base.actual_end_time_work < base.h8_start then 0
    when base.actual_start_time_work > base.h8_end then 0
    else date_diff('second',   greatest(base.h8_start,base.actual_start_time_work)   ,   least(base.h8_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h8_working_time
 ,case when base.actual_end_time_work < base.h9_start then 0
    when base.actual_start_time_work > base.h9_end then 0
    else date_diff('second',   greatest(base.h9_start,base.actual_start_time_work)   ,   least(base.h9_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h9_working_time  
 ,case when base.actual_end_time_work < base.h10_start then 0
    when base.actual_start_time_work > base.h10_end then 0
    else date_diff('second',   greatest(base.h10_start,base.actual_start_time_work)   ,   least(base.h10_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h10_working_time  
 ,case when base.actual_end_time_work < base.h11_start then 0
    when base.actual_start_time_work > base.h11_end then 0
    else date_diff('second',   greatest(base.h11_start,base.actual_start_time_work)   ,   least(base.h11_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h11_working_time  
 ,case when base.actual_end_time_work < base.h12_start then 0
    when base.actual_start_time_work > base.h12_end then 0
    else date_diff('second',   greatest(base.h12_start,base.actual_start_time_work)   ,   least(base.h12_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h12_working_time  
 ,case when base.actual_end_time_work < base.h13_start then 0
    when base.actual_start_time_work > base.h13_end then 0
    else date_diff('second',   greatest(base.h13_start,base.actual_start_time_work)   ,   least(base.h13_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h13_working_time  
 ,case when base.actual_end_time_work < base.h14_start then 0
    when base.actual_start_time_work > base.h14_end then 0
    else date_diff('second',   greatest(base.h14_start,base.actual_start_time_work)   ,   least(base.h14_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h14_working_time  
 ,case when base.actual_end_time_work < base.h15_start then 0
    when base.actual_start_time_work > base.h15_end then 0
    else date_diff('second',   greatest(base.h15_start,base.actual_start_time_work)   ,   least(base.h15_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h15_working_time  
 ,case when base.actual_end_time_work < base.h16_start then 0
    when base.actual_start_time_work > base.h16_end then 0
    else date_diff('second',   greatest(base.h16_start,base.actual_start_time_work)   ,   least(base.h16_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h16_working_time  
 ,case when base.actual_end_time_work < base.h17_start then 0
    when base.actual_start_time_work > base.h17_end then 0
    else date_diff('second',   greatest(base.h17_start,base.actual_start_time_work)   ,   least(base.h17_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h17_working_time  
 ,case when base.actual_end_time_work < base.h18_start then 0
    when base.actual_start_time_work > base.h18_end then 0
    else date_diff('second',   greatest(base.h18_start,base.actual_start_time_work)   ,   least(base.h18_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h18_working_time  
 ,case when base.actual_end_time_work < base.h19_start then 0
    when base.actual_start_time_work > base.h19_end then 0
    else date_diff('second',   greatest(base.h19_start,base.actual_start_time_work)   ,   least(base.h19_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h19_working_time  
 ,case when base.actual_end_time_work < base.h20_start then 0
    when base.actual_start_time_work > base.h20_end then 0
    else date_diff('second',   greatest(base.h20_start,base.actual_start_time_work)   ,   least(base.h20_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h20_working_time  
 ,case when base.actual_end_time_work < base.h21_start then 0
    when base.actual_start_time_work > base.h21_end then 0
    else date_diff('second',   greatest(base.h21_start,base.actual_start_time_work)   ,   least(base.h21_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h21_working_time  
 ,case when base.actual_end_time_work < base.h22_start then 0
    when base.actual_start_time_work > base.h22_end then 0
    else date_diff('second',   greatest(base.h22_start,base.actual_start_time_work)   ,   least(base.h22_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h22_working_time  
 ,case when base.actual_end_time_work < base.h23_start then 0
    when base.actual_start_time_work > base.h23_end then 0
    else date_diff('second',   greatest(base.h23_start,base.actual_start_time_work)   ,   least(base.h23_end,base.actual_end_time_work)   )*1.0000/(60*60)
    end as h23_working_time  




from
(SELECT uid as shipper_id
,date(from_unixtime(create_time - 60*60)) as create_date

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
-- need to recheck if work_time = 0 then work distance should be = 0
,total_work_distance*1.000/1000 as total_work_distance 
,total_completed_order
,total_delivered_order
,total_cancelled_order
,total_quit_order
,total_denied_order
,total_completed_auto_assign_order
,total_earned_points

-- actual use
,from_unixtime(check_in_time - 60*60) as actual_start_time_online
,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online
,from_unixtime(check_out_time - 60*60) as test_1
,from_unixtime(order_end_time - 60*60) as test_2

,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work

--by hour slot
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '0' hour as h0_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '1' hour as h0_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '1' hour as h1_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '2' hour as h1_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '2' hour as h2_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '3' hour as h2_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '3' hour as h3_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '4' hour as h3_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '4' hour as h4_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '5' hour as h4_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '5' hour as h5_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '6' hour as h5_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '6' hour as h6_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '7' hour as h6_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '7' hour as h7_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '8' hour as h7_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '8' hour as h8_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '9' hour as h8_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '9' hour as h9_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '10' hour as h9_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '10' hour as h10_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '11' hour as h10_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '11' hour as h11_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '12' hour as h11_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '12' hour as h12_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '13' hour as h12_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '13' hour as h13_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '14' hour as h13_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '14' hour as h14_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '15' hour as h14_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '15' hour as h15_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '16' hour as h15_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '16' hour as h16_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '17' hour as h16_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '17' hour as h17_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '18' hour as h17_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '18' hour as h18_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '19' hour as h18_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '19' hour as h19_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '20' hour as h19_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '20' hour as h20_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '21' hour as h20_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '21' hour as h21_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '22' hour as h21_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '22' hour as h22_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '23' hour as h22_end

,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '23' hour as h23_start
,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '24' hour as h23_end


from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts
where 1=1
--and sts.uid = 2576025
--and date(from_unixtime(create_time - 60*60)) between DATE'2021-11-11' - interval '30' day and DATE'2021-11-11'
--and (date(from_unixtime(create_time - 60*60))  = date('2022-03-08') or date(from_unixtime(create_time - 60*60))  = date('2022-04-04') or date(from_unixtime(create_time - 60*60))  = date('2021-12-12') or (date(from_unixtime(create_time - 60*60)) >= date('2022-03-28') and date(from_unixtime(create_time - 60*60)) <= date('2022-04-03')))
and check_in_time > 0
and check_out_time > 0
and check_out_time >= check_in_time
and order_end_time >= order_start_time
and ((order_start_time = 0 and order_end_time = 0)
    OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
    )

)base
    -- location: taken from location of last order of the day
    
    LEFT JOIN (SELECT oct.shipper_uid as shipper_id
                ,cast(from_unixtime(oct.submit_time - 60*60) as date) as created_date
                ,from_unixtime(oct.submit_time - 60*60) as created_timestamp
                ,case when oct.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                ,case when oct.city_id  = 217 then 'HCM'
                        when oct.city_id  = 218 then 'HN'
                        when oct.city_id  = 219 then 'DN'
                        ELSE 'OTH' end as city_group
                
                from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
                left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live filter on filter.shipper_uid = oct.shipper_uid
                                                                                and cast(from_unixtime(filter.submit_time - 60*60) as date) = cast(from_unixtime(oct.submit_time - 60*60) as date)
                                                                                and filter.status = 7
                                                                                and from_unixtime(filter.submit_time - 60*60) > from_unixtime(oct.submit_time - 60*60)
                                                                                
                -- location
                    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86
                
                where 1=1
               -- and cast(from_unixtime(oct.submit_time - 60*60) as date) between date('2020-01-07') and date('2020-01-07')
                and oct.status = 7
                and filter.id is null
                
                GROUP BY 1,2,3,4,5
            )loc on loc.shipper_id = base.shipper_id and loc.created_date = base.create_date
            
    -- extra location        
    LEFT JOIN (SELECT shipper_id
                ,city_name
                ,case when city_name = 'HCM City' then 'HCM'
                    when city_name = 'Ha Noi City' then 'HN'  
                    when city_name = 'Da Nang City' then 'DN'
                    else 'OTH' end as city_group
                ,case when grass_date = 'current' then date(current_date)
                    else cast(grass_date as date) end as report_date
                
                from shopeefood.foody_mart__profile_shipper_master
                
                where 1=1 
                and (TRY_CAST(grass_date AS DATE) = DATE'2022-03-08' OR TRY_CAST(grass_date AS DATE) >= date('2019-01-01'))
                 GROUP BY 1,2,3,4
                )loc_extra on loc_extra.shipper_id = base.shipper_id and loc_extra.report_date = base.create_date
                
    -- driver Tier
    LEFT JOIN (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
                    ,bonus.uid as shipper_id
                    ,case when bonus.total_point <= 1800 then 'T1'
                        when bonus.total_point <= 3600 then 'T2'
                        when bonus.total_point <= 5400 then 'T3'
                        when bonus.total_point <= 8400 then 'T4'
                        when bonus.total_point > 8400 then 'T5'
                        else null end as new_driver_tier   
                    
                    ,case when hub.shipper_type_id = 12 then 'Hub' 
                        when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
                        when bonus.tier in (2,7,12) then 'T2'
                        when bonus.tier in (3,8,13) then 'T3'
                        when bonus.tier in (4,9,14) then 'T4'
                        when bonus.tier in (5,10,15) then 'T5'
                        else null end as current_driver_tier
                    ,bonus.total_point    
                
                FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
                
                LEFT JOIN 
                            (SELECT shipper_id
                                ,shipper_type_id
                                ,case when grass_date = 'current' then date(current_date)
                                    else cast(grass_date as date) end as report_date
                                
                                from shopeefood.foody_mart__profile_shipper_master
                                
                                where 1=1 
                                and (TRY_CAST(grass_date AS DATE) = DATE'2022-03-08' OR TRY_CAST(grass_date AS DATE) >= date('2019-01-01'))
                                 GROUP BY 1,2,3
                            )hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)
                
            )bonus on base.create_date = bonus.report_date and base.shipper_id = bonus.shipper_id
             
    
   
)base1           
where create_date between date'2021-04-14' and date'2021-05-05'
GROUP BY 1,2,3,4,5,6,7