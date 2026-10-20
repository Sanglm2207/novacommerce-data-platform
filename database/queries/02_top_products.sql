
-- DATA-001 / T03.2

SELECT
    p.product_id,
    p.product_name,

    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count
FROM products AS p
JOIN order_items AS oi
    ON oi.product_id = p.product_id
JOIN orders AS o
    ON o.order_id = oi.order_id
WHERE o.status = 'COMPLETED'
  AND EXISTS (
      SELECT 1
      FROM payments AS pay
      WHERE pay.order_id = o.order_id
        AND pay.status = 'SUCCESS'
  )
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC, p.product_id ASC
LIMIT 10;
