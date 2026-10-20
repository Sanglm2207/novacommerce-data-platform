-- DATA-001 / T03.4
-- Revenue uses the UTC order creation date, not the payment/completion date.
-- Cover the first through last order date, including days with zero revenue.

WITH eligible_orders AS (
    SELECT
        o.order_id,
        (o.created_at AT TIME ZONE 'UTC')::date AS revenue_date
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
        eo.revenue_date,
        SUM(oi.quantity * oi.unit_price) AS order_total
    FROM eligible_orders AS eo
    JOIN order_items AS oi
        ON oi.order_id = eo.order_id
    GROUP BY eo.order_id, eo.revenue_date
),
daily_totals AS (
    SELECT revenue_date, SUM(order_total) AS daily_revenue
    FROM order_totals
    GROUP BY revenue_date
),
date_bounds AS (
    SELECT
        MIN((created_at AT TIME ZONE 'UTC')::date) AS first_date,
        MAX((created_at AT TIME ZONE 'UTC')::date) AS last_date
    FROM orders
),
calendar AS (
    SELECT b.first_date + s.day_offset AS revenue_date
    FROM date_bounds AS b
    CROSS JOIN LATERAL generate_series(
        0, b.last_date - b.first_date
    ) AS s(day_offset)
),
daily_revenue AS (
    SELECT
        c.revenue_date,
        COALESCE(dt.daily_revenue, 0::numeric) AS daily_revenue
    FROM calendar AS c
    LEFT JOIN daily_totals AS dt
        ON dt.revenue_date = c.revenue_date
),
revenue_metrics AS (
    SELECT
        revenue_date,
        daily_revenue,
        LAG(daily_revenue) OVER (
            ORDER BY revenue_date
        ) AS previous_day_revenue,
        -- The first six dates average only the available days.
        AVG(daily_revenue) OVER (
            ORDER BY revenue_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7_days
    FROM daily_revenue
)
SELECT
    revenue_date,
    daily_revenue,
    previous_day_revenue,
    ROUND(
        100.0 * (daily_revenue - previous_day_revenue)
        / NULLIF(previous_day_revenue, 0),
        2
    ) AS dod_growth_pct,
    moving_avg_7_days
FROM revenue_metrics
ORDER BY revenue_date;
