
-- DATA-001 / T03.1
-- Goal: Calculate daily revenue accurately.
-- Note: This is a starter, not the complete solution.

WITH eligible_orders AS (
    SELECT
        o.order_id,
        o.created_at
    FROM orders AS o
    WHERE o.status = 'COMPLETED'
      AND EXISTS (
          SELECT 1
          FROM payments AS p
          WHERE p.order_id = o.order_id
            AND p.status = 'SUCCESS'
      )
),
order_totals AS (
    SELECT
        eo.order_id,
        eo.created_at,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.quantity * oi.unit_price) AS order_total
    FROM eligible_orders AS eo
    JOIN order_items AS oi
        ON oi.order_id = eo.order_id
    GROUP BY eo.order_id, eo.created_at
)

SELECT
    (ot.created_at AT TIME ZONE 'UTC')::date AS order_date,
    COUNT(*) AS order_count,
    SUM(ot.units_sold) AS total_units_sold,
    SUM(ot.order_total) AS gross_revenue,
    AVG(ot.order_total) AS avg_order_value
FROM order_totals AS ot
GROUP BY (ot.created_at AT TIME ZONE 'UTC')::date
ORDER BY order_date DESC
LIMIT 10;
