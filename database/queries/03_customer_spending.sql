-- DATA-001 / T03.3
-- Historical customer spending, not predicted lifetime value.
-- Include every customer; average order value is NULL without eligible orders.

WITH eligible_orders AS (
    SELECT o.order_id, o.customer_id
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
        eo.customer_id,
        SUM(oi.quantity * oi.unit_price) AS order_total
    FROM eligible_orders AS eo
    JOIN order_items AS oi
        ON oi.order_id = eo.order_id
    GROUP BY eo.order_id, eo.customer_id
),
customer_spending AS (
    SELECT
        c.customer_id,
        c.full_name,
        COUNT(ot.order_id) AS completed_orders,
        COALESCE(SUM(ot.order_total), 0) AS total_spending,
        AVG(ot.order_total) AS avg_order_value
    FROM customers AS c
    LEFT JOIN order_totals AS ot
        ON ot.customer_id = c.customer_id
    GROUP BY c.customer_id, c.full_name
)
SELECT
    customer_id,
    full_name,
    completed_orders,
    total_spending,
    avg_order_value,
    DENSE_RANK() OVER (ORDER BY total_spending DESC) AS customer_rank
FROM customer_spending
ORDER BY customer_rank, customer_id;
