-- Read-only validation; expected discrepancies = 0 after a full initial load.
SELECT (SELECT COUNT(*) FROM public.orders) AS source_orders,
       (SELECT COUNT(*) FROM staging.orders) AS staging_orders;

SELECT COUNT(*) AS missing_in_staging
FROM public.orders o
LEFT JOIN staging.orders s ON s.order_id = o.order_id
WHERE s.order_id IS NULL;

SELECT COUNT(*) AS mismatched_order_values
FROM staging.orders s
JOIN public.orders o ON o.order_id = s.order_id
WHERE s.order_status IS DISTINCT FROM o.status
   OR s.customer_id IS DISTINCT FROM o.customer_id
   OR s.order_date IS DISTINCT FROM o.created_at
   OR s.total_items IS DISTINCT FROM (
      SELECT COALESCE(SUM(oi.quantity), 0) FROM public.order_items oi
      WHERE oi.order_id = o.order_id
   )
   OR s.total_amount IS DISTINCT FROM (
      SELECT COALESCE(SUM(oi.quantity * oi.unit_price), 0) FROM public.order_items oi
      WHERE oi.order_id = o.order_id
   )
   OR s.has_successful_payment IS DISTINCT FROM EXISTS (
      SELECT 1 FROM public.payments p
      WHERE p.order_id = o.order_id AND p.status = 'SUCCESS'
   );

SELECT pipeline_name, last_created_at, last_order_id
FROM etl.checkpoints WHERE pipeline_name = 'orders_pipeline';
