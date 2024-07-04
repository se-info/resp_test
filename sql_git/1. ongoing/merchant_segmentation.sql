select 
        grass_month,
        merchant_id,
        if(is_food_merchant = 1, segment, 'Mart') AS segment

from vnfdbi_commercial.spf_dwd_mex_segment_monthly_vn

WHERE grass_month = date'2024-05-01'