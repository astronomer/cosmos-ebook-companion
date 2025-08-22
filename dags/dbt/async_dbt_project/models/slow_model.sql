WITH large_computation AS (
  SELECT 
    i,
    j,
    POW(i, 2) + POW(j, 2) as sum_of_squares,
    SQRT(i * j) as sqrt_product,
    SIN(i) * COS(j) as trig_calc,
    LOG(i + 1) * LOG(j + 1) as log_product
  FROM UNNEST(GENERATE_ARRAY(1, 2000)) AS i
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, 2000)) AS j
  WHERE MOD(i + j, 100) = 0  -- Reduce but still keep significant computation
),
aggregated AS (
  SELECT 
    COUNT(*) as total_rows,
    AVG(sum_of_squares) as avg_sum_squares,
    SUM(sqrt_product) as total_sqrt_product,
    MAX(trig_calc) as max_trig,
    MIN(log_product) as min_log_product
  FROM large_computation
)
SELECT 
  5 as my_number,
  total_rows,
  ROUND(avg_sum_squares, 2) as avg_sum_squares,
  ROUND(total_sqrt_product, 2) as total_sqrt_product,
  ROUND(max_trig, 4) as max_trig,
  ROUND(min_log_product, 4) as min_log_product,
  'This query should take about 20 seconds' as message
FROM aggregated 