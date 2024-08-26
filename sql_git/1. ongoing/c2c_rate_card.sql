with rate_card(from_distance,to_distance,current_fee,new_fee) as 
(VALUES
(0,2000,17000,17000),
(2000,2500,17000,17000),
(2500,3000,19000,18000),
(3000,3500,19000,19000),
(3500,4000,24000,21500),
(4000,4500,24000,24000),
(4500,5000,29000,26500),
(5000,5500,29000,29000),
(5500,6000,34000,31500),
(6000,6500,34000,34000),
(6500,7000,40000,37000),
(7000,7500,40000,40000),
(7500,8000,46000,43000),
(8000,8500,46000,46000),
(8500,9000,52000,49000),
(9000,9500,52000,52000),
(9500,10000,58000,55000),
(10000,11000,58000,58000),
(11000,12000,63500,63500),
(12000,13000,69000,69000),
(13000,14000,74500,74500),
(14000,15000,80000,80000),
(15000,16000,85500,85500),
(16000,17000,91000,91000),
(17000,18000,96500,96500),
(18000,19000,102000,102000),
(19000,20000,107500,107500),
(20000,21000,113000,113000),
(21000,22000,118500,118500),
(22000,23000,124000,124000),
(23000,24000,129500,129500),
(24000,25000,135000,135000),
(25000,26000,140500,140500),
(26000,27000,146000,146000),
(27000,28000,151500,151500),
(28000,29000,157000,157000),
(29000,30000,162500,162500),
(30000,31000,168000,168000),
(31000,9999,173500,173500)
)
select 
        date(from_unixtime(create_time - 3600)) as created,
        o.id,
        o.code,
        o.distance,
        r.new_fee,
        json_extract(o.extra_data,'$.shipping_fee.shipping_fee_origin') as user_shipping_original,
        cast(json_extract(o.extra_data,'$.shipping_fee.rate') as double) as surge_rate,
        cast(json_extract(o.extra_data,'$.shipping_fee.shipping_fee_origin') as bigint)/
            cast(json_extract(o.extra_data,'$.shipping_fee.rate') as double) as shipping_fee_no_surge    


-- select *
from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live o 
-- where cast(json_extract(o.extra_data,'$.shipping_fee.rate') as double) > 1

left join rate_card r on o.distance > r.from_distance and o.distance <= to_distance
where status = 11
and date(from_unixtime(create_time - 3600)) between date'2024-08-01' and date'2024-08-14'
order by 1 desc
;