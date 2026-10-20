-- UTC order creation date, not payment settlement date.
-- Excludes cancelled/incomplete/unpaid orders under current training rules.
SELECT
  d.full_date AS revenue_date,
  COUNT(DISTINCT f.order_id) AS completed_orders,
  SUM(f.quantity) AS units_sold,
  SUM(f.line_amount) AS gross_revenue
FROM warehouse.fact_order_items AS f
JOIN warehouse.dim_date AS d ON d.date_key = f.order_date_key
WHERE f.order_status = 'COMPLETED'
  AND f.has_successful_payment
GROUP BY d.full_date
ORDER BY revenue_date DESC
LIMIT 30;
