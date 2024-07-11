select
    city
    ,hub_location
    ,"day_of_week"
    ,case
    when cast("day_of_week" as bigint ) = 1 then date'2024-07-15'
    when cast("day_of_week" as bigint ) = 2 then date'2024-07-15' + interval '1' day 
    when cast("day_of_week" as bigint ) = 3 then date'2024-07-15' + interval '2' day
    when cast("day_of_week" as bigint ) = 4 then date'2024-07-15' + interval '3' day
    when cast("day_of_week" as bigint ) = 5 then date'2024-07-15' + interval '4' day
    when cast("day_of_week" as bigint ) = 6 then date'2024-07-15' + interval '5' day
    when cast("day_of_week" as bigint ) = 7 then date'2024-07-15' + interval '6' day
    end as slot_date
    ,"3_hour_shift_0"
    ,"3_hour_shift_3"
    ,"3_hour_shift_6"
    ,"3_hour_shift_16"
    ,"3_hour_shift_19"
    ,"3_hour_shift_20"
    ,"3_hour_shift_21"
    ,"5_hour_shift_6"
    ,"5_hour_shift_8"
    ,"5_hour_shift_11"
    ,"5_hour_shift_16"
    ,"5_hour_shift_18"
    ,"8_hour_shift_11"
    ,"10_hour_shift_10"
    ,calculation_seed
    ,updated_date
from shopeefood_assignment.hcm_slot_schedule_tab
where updated_date = '20240708'
;
select
    city
    ,hub_location
    ,"day_of_week"
    ,case
    when cast("day_of_week" as bigint ) = 1 then date'2024-07-15'
    when cast("day_of_week" as bigint ) = 2 then date'2024-07-15' + interval '1' day 
    when cast("day_of_week" as bigint ) = 3 then date'2024-07-15' + interval '2' day
    when cast("day_of_week" as bigint ) = 4 then date'2024-07-15' + interval '3' day
    when cast("day_of_week" as bigint ) = 5 then date'2024-07-15' + interval '4' day
    when cast("day_of_week" as bigint ) = 6 then date'2024-07-15' + interval '5' day
    when cast("day_of_week" as bigint ) = 7 then date'2024-07-15' + interval '6' day
    end as slot_date
    ,"3_hour_shift_0"
    ,"3_hour_shift_3"
    ,"3_hour_shift_7"
    ,"3_hour_shift_10"
    ,"3_hour_shift_16"
    ,"3_hour_shift_18"
    ,"3_hour_shift_20"
    ,"3_hour_shift_21"
    ,"5_hour_shift_6"
    ,"5_hour_shift_8"
    ,"5_hour_shift_11"
    ,"5_hour_shift_16"
    ,"5_hour_shift_18"
    ,"8_hour_shift_11"
    ,"10_hour_shift_10"
    ,calculation_seed
    ,updated_date
from shopeefood_assignment.hn_slot_schedule_tab
where updated_date = '20240708'
