SELECT  
        FROM_UNIXTIME(create_time - 3600),
        id,
        shift_category,
        CAST(start_time / 3600 AS VARCHAR) || ':' ||  -- Hours
        CAST((start_time % 3600) / 60 AS VARCHAR) || ':' || -- Minutes
        CAST(start_time % 60 AS VARCHAR) AS start_time, -- Seconds

        CAST(end_time / 3600 AS VARCHAR) || ':' ||  -- Hours
        CAST((end_time % 3600) / 60 AS VARCHAR) || ':' || -- Minutes
        CAST(end_time % 60 AS VARCHAR) AS end_time -- Seconds

FROM shopeefood.foody_internal_db__shipper_config_working_time_tab__vn_daily_s0_live

order by 1 desc 