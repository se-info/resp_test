SELECT CAST(TRIM(hub_id) AS INT) hub_id
FROM
(
(
SELECT split('6, 7, 9, 19, 20, 25, 26, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 44, 45, 46, 66, 91, 132, 127, 130',',') as a
)
CROSS JOIN unnest(a) AS t(hub_id)
)
