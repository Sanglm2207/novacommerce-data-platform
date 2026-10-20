-- Run after Day 06 CDC sync and Day 10 incremental load; expect all 0.
SELECT 'multiple_current_versions' AS check_name,COUNT(*) AS violations
FROM (SELECT product_id FROM warehouse.dim_product_history
      WHERE is_current GROUP BY product_id HAVING COUNT(*) <> 1) x
UNION ALL
SELECT 'history_overlaps',COUNT(*) FROM warehouse.dim_product_history a
JOIN warehouse.dim_product_history b
  ON a.product_id=b.product_id AND a.product_version_key<b.product_version_key
 AND a.valid_from<COALESCE(b.valid_to,'infinity'::timestamptz)
 AND b.valid_from<COALESCE(a.valid_to,'infinity'::timestamptz)
UNION ALL
SELECT 'missing_current_products',COUNT(*) FROM public.products p
LEFT JOIN warehouse.dim_product_history h ON h.product_id=p.product_id AND h.is_current
WHERE h.product_version_key IS NULL
UNION ALL
SELECT 'unprocessed_product_events',COUNT(*) FROM warehouse.product_events WHERE processed_at IS NULL
UNION ALL
SELECT 'unprocessed_refresh_events',COUNT(*) FROM warehouse.refresh_queue WHERE processed_at IS NULL
UNION ALL
SELECT 'missing_facts',COUNT(*) FROM public.order_items oi
JOIN staging.orders s ON s.order_id=oi.order_id
LEFT JOIN warehouse.fact_order_items f ON f.order_item_id=oi.order_item_id
WHERE f.order_item_id IS NULL
UNION ALL
SELECT 'stale_facts',COUNT(*) FROM warehouse.fact_order_items f
JOIN staging.orders s ON s.order_id=f.order_id
JOIN public.order_items oi ON oi.order_item_id=f.order_item_id
WHERE f.order_status IS DISTINCT FROM s.order_status
   OR f.has_successful_payment IS DISTINCT FROM s.has_successful_payment
   OR f.quantity IS DISTINCT FROM oi.quantity
   OR f.unit_price IS DISTINCT FROM oi.unit_price;
