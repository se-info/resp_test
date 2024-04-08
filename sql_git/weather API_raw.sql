
SELECT   base.id 
        ,base.city_id
        ,base.district_id
        ,base.weather 
        ,base.is_rain_negative
        ,base.is_rain
        ,base.create_time as start_time
        ,base.running_time
        ,base.end_time


from
        (SELECT all.id 
        ,all.city_id
        ,all.district_id
        ,all.weather 
        ,all.create_time
        ,case when all.is_rain = 'true' then 1 else 0 end as is_rain
        ,date_diff('second',all.create_time,next_call.create_time) as running_time
        ,next_call.create_time as end_time
        ,case when all.weather in ('Rain','Thundershower','Heavy thunderstorm','Thunderstorm',
                                    'Heavy rain','Rain shower','Heavy rain shower','A shower') then 1 else 0 end as is_rain_negative
        
        
        
        from
                (
                SELECT   adt.id
                        ,adt.city_id
                        ,adt.district_id
                        ,trim(replace(cast(json_extract(adt.weather_data,'$[0].WeatherText') as VARCHAR),'"')) as weather
                        ,cast(json_extract(adt.weather_data,'$[0].HasPrecipitation') as VARCHAR) as is_rain           
                        ,from_unixtime(adt.query_time - 60*60) as query_time
                        ,from_unixtime(adt.create_time - 60*60) as create_time       
                
                from shopeefood.foody_geo_db__accuweather_data_tab__reg_daily_s0_live adt
                            
                where 1=1
                and date(from_unixtime(adt.create_time - 60*60)) between date('2021-01-01') and date(now()) - interval '1' day
                )all
    
        LEFT JOIN 
                (
                SELECT   id
                        ,city_id
                        ,district_id
                        ,trim(replace(cast(json_extract(weather_data,'$[0].WeatherText') as VARCHAR),'"')) as weather
                        ,cast(json_extract(weather_data,'$[0].HasPrecipitation') as VARCHAR) as is_rain
                        ,from_unixtime(query_time - 60*60) as query_time
                        ,from_unixtime(create_time - 60*60) as create_time
                 
                from shopeefood.foody_geo_db__accuweather_data_tab__reg_daily_s0_live
                where 1=1
                and date(from_unixtime(create_time - 60*60)) between date('2021-01-01') and date(now()) - interval '1' day
                )next_call on next_call.city_id = all.city_id and next_call.district_id = all.district_id and next_call.create_time > all.create_time
                                                                                                            and next_call.create_time < all.create_time + interval '1' hour    
    
        LEFT JOIN 
                (
                SELECT id
                    ,city_id
                    ,district_id
                    ,trim(replace(cast(json_extract(weather_data,'$[0].WeatherText') as VARCHAR),'"')) as weather
                    ,cast(json_extract(weather_data,'$[0].HasPrecipitation') as VARCHAR) as is_rain
                    ,from_unixtime(query_time - 60*60) as query_time
                    ,from_unixtime(create_time - 60*60) as create_time        
                        
                from shopeefood.foody_geo_db__accuweather_data_tab__reg_daily_s0_live
                where 1=1
                and date(from_unixtime(create_time - 60*60)) between date('2021-01-01') and date(now()) - interval '1' day
                )next_call_filter on next_call_filter.city_id = all.city_id and next_call_filter.district_id = all.district_id 
                                                                            and next_call_filter.create_time > all.create_time 
                                                                            and next_call_filter.create_time < all.create_time + interval '1' hour
                                                                            and next_call_filter.create_time < next_call.create_time
        where 1=1
        and next_call_filter.id is null
        )base

where base.running_time is not null 