
-- DATA-001 / T03.5
-- Query Performance Optimization

-- ============================================
-- STEP 1: Baseline (Before Index)
-- ============================================

EXPLAIN (ANALYZE, BUFFERS)
SELECT
    o.customer_id,
    COUNT(*) AS total_orders
FROM orders AS o
WHERE o.status = 'COMPLETED'
GROUP BY o.customer_id;


-- ============================================
-- STEP 2: Create Partial Index
-- ============================================

CREATE INDEX IF NOT EXISTS idx_orders_completed_customer
ON orders(customer_id)
WHERE status = 'COMPLETED';

ANALYZE orders;


-- ============================================
-- STEP 3: Benchmark (After Index)
-- ============================================

EXPLAIN (ANALYZE, BUFFERS)
SELECT
    o.customer_id,
    COUNT(*) AS total_orders
FROM orders AS o
WHERE o.status = 'COMPLETED'
GROUP BY o.customer_id;
