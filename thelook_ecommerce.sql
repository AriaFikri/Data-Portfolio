-- Q: What are the top 10 best selling product category by revenue
SELECT p.category, ROUND(SUM(oi.sale_price),2) total_sales
FROM bigquery-public-data.thelook_ecommerce.products p
JOIN bigquery-public-data.thelook_ecommerce.order_items oi
  ON p.id = oi.product_id
WHERE oi.status = "Complete"
GROUP BY p.category
ORDER BY SUM(oi.sale_price) DESC
LIMIT 10;

-- which age group has the highest avg revenue per order

WITH cte AS (
  SELECT
    u.id,
    oi.order_id order_id,
    u.age age, 
    CASE
      WHEN u.age BETWEEN 12 AND 19 THEN "TEEN"
      WHEN u.age BETWEEN 20 AND 39 THEN "YOUNG-MIDDLE"
      WHEN u.age BETWEEN 40 AND 59 THEN "ADULT-LATE"
      WHEN u.age BETWEEN 60 AND 70 THEN "SENIOR"
    END AS age_bin,
    SUM(oi.sale_price) total_price,
    oi.status status
  FROM bigquery-public-data.thelook_ecommerce.users u
  JOIN bigquery-public-data.thelook_ecommerce.order_items oi
    ON oi.user_id = u.id
  JOIN bigquery-public-data.thelook_ecommerce.orders o
    ON o.order_id = oi.order_id
  GROUP BY u.id, oi.order_id, u.age, age_bin, oi.status
)
SELECT 
  age_bin, 
  COUNT(DISTINCT order_id) order_freq, 
  ROUND(AVG(total_price),2) avg_revenue,
  ROUND(SUM(total_price),2) total_revenue
FROM 
  cte
WHERE 
  status = "Complete"
GROUP BY 
  age_bin
ORDER BY 
  ROUND(SUM(total_price),2) DESC;

-- 30 day retention rate for users who signed up in Jan 2023
WITH jan_users AS(
  SELECT id, created_at
  FROM bigquery-public-data.thelook_ecommerce.users
  WHERE created_at BETWEEN '2023-01-01' AND '2023-01-31'
  ORDER BY created_at DESC
),

retained_users AS(
  SELECT DISTINCT ju.id id
  FROM jan_users ju
  JOIN bigquery-public-data.thelook_ecommerce.orders o
    ON ju.id = o.user_id
  WHERE 
    o.created_at 
    BETWEEN DATE_ADD(ju.created_at, INTERVAL 30 DAY) AND DATE_ADD(ju.created_at, INTERVAL 60 DAY)
)

SELECT COUNT(DISTINCT r.id) retained, COUNT(DISTINCT j.id) jan_user
FROM jan_users j
LEFT JOIN retained_users r
  ON j.id = r.id;

-- monthly revenue growth
WITH cte AS(
SELECT 
  EXTRACT(year FROM created_at) year_order, 
  EXTRACT(month FROM created_at) month_order, 
  ROUND(SUM(sale_price),2) monthly_sale
FROM 
  bigquery-public-data.thelook_ecommerce.order_items
WHERE 
  status = "Complete"
GROUP BY 
  year_order, 
  month_order
ORDER BY 
  year_order ASC, 
  month_order ASC
), 
cte2 AS (
SELECT 
  *, 
  ROUND(SUM(monthly_sale) OVER (ORDER BY year_order ASC, month_order ASC),2) running_total,
  LAG(monthly_sale, 1) OVER (ORDER BY year_order ASC, month_order ASC) lag_total
FROM cte
)
SELECT *, ROUND((monthly_sale - lag_total)/lag_total, 2) * 100 sales_growth_monthly
FROM cte2
ORDER BY year_order ASC, month_order ASC;

-- which shipping center has the fastest avg delivery time?
WITH cte AS (
SELECT 
  ii.product_distribution_center_id dist_id, 
  dc.name dist_name, 
  ii.product_id prod_id,
  oi.status,
  oi.shipped_at,
  oi.delivered_at
FROM 
  bigquery-public-data.thelook_ecommerce.inventory_items ii
LEFT JOIN bigquery-public-data.thelook_ecommerce.distribution_centers dc
  ON ii.product_distribution_center_id = dc.id
JOIN bigquery-public-data.thelook_ecommerce.order_items  oi
  ON oi.product_id = ii.product_id
)
SELECT DISTINCT
  dist_id, 
  dist_name, 
  ROUND(AVG(DATE_DIFF(delivered_at, shipped_at, hour)) OVER (PARTITION BY dist_id),2) avg_delivery_time
FROM 
  cte
WHERE 
  status = "Complete"
ORDER BY 
  avg_delivery_time DESC;

-- Calculate recency, frequency, and monetary metrics for every customer?
WITH cte as (
SELECT
  u.id user_id,
  COUNT(DISTINCT o.order_id) order_freq,
  EXTRACT(DATE FROM MAX(o.created_at)) last_order,
  ROUND(SUM(oi.sale_price),2) monetary
FROM 
  bigquery-public-data.thelook_ecommerce.orders o
LEFT JOIN 
  bigquery-public-data.thelook_ecommerce.users u
    ON u.id = o.user_id
JOIN 
  bigquery-public-data.thelook_ecommerce.order_items oi
    ON oi.order_id = o.order_id
WHERE
  o.status = "Shipped"
GROUP BY 
  user_id
ORDER BY 
  user_id
), cte2 AS (
SELECT 
  *, 
  DATE_DIFF(MAX(last_order) OVER(), last_order, DAY) days_since_order,
  NTILE(10) OVER (ORDER BY monetary ASC) mon_score, 
  order_freq AS freq_score
FROM 
  cte
ORDER BY 
  order_freq DESC
)
SELECT *, NTILE(10) OVER (ORDER BY days_since_order DESC) recency_score
FROM cte2
ORDER BY order_freq;

-- which products have the highest profit margins?
WITH cte as (
SELECT
  oi.product_id,
  oi.order_id,
  ROUND(p.cost,3) cost,
  ROUND(oi.sale_price,3) sale_price,
  ROUND(sale_price-cost, 3) profit
FROM 
  bigquery-public-data.thelook_ecommerce.order_items oi
LEFT JOIN bigquery-public-data.thelook_ecommerce.products p
  ON oi.product_id = p.id
)
SELECT DISTINCT
  product_id,
  AVG(profit) avg_profit
FROM 
  cte
GROUP BY
  product_id
ORDER BY 
  avg_profit DESC
