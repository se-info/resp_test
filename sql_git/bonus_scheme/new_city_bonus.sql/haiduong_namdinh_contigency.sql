WITH filter_list AS 
(SELECT 
        shipper_id,
        COUNT(DISTINCT report_date) AS working_days,
        COUNT(DISTINCT CASE WHEN total_order >= 15 THEN report_date ELSE NULL END) AS ado_qualified
FROM driver_ops_driver_performance_tab
WHERE report_date BETWEEN DATE'2024-04-27' AND DATE'2024-04-30'
AND total_order > 0 
AND city_name IN ('Hai Duong','Nam Dinh City')
GROUP BY 1
HAVING COUNT(DISTINCT report_date) = 4
)

SELECT 
        raw.report_date,
        raw.shipper_id,
        sm.shipper_name,
        raw.city_name,
        raw.total_order,
        fl.working_days,
        CASE 
        WHEN raw.total_order >= 15 THEN 40000 
        ELSE 0 END AS bonus_value,
        'Thuong chuong trinh khuyen khich tai xe '||DATE_FORMAT(raw.report_date,'%d/%m/%Y') AS note_


FROM driver_ops_driver_performance_tab raw 

LEFT JOIN (SELECT shipper_id,shipper_name FROM shopeefood.foody_mart__profile_sh    ipper_master WHERE grass_date = 'current') sm on sm.shipper_id = raw.shipper_id 

INNER JOIN filter_list fl on fl.shipper_id = raw.shipper_id

WHERE raw.report_date BETWEEN DATE'2024-04-27' AND DATE'2024-04-30'


