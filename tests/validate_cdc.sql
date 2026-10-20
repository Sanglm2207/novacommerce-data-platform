SELECT COUNT(*) AS pending_events FROM etl.order_changes WHERE processed_at IS NULL;
SELECT COUNT(*) AS processed_events FROM etl.order_changes WHERE processed_at IS NOT NULL;
SELECT COUNT(*) AS source_orders FROM public.orders;
SELECT COUNT(*) AS staging_orders FROM staging.orders;
SELECT COUNT(*) AS missing_staging_orders
FROM public.orders o LEFT JOIN staging.orders s USING(order_id)
WHERE s.order_id IS NULL;
SELECT COUNT(*) AS mismatched_staging_orders
FROM staging.orders s
JOIN public.orders o ON o.order_id=s.order_id
WHERE s.order_status IS DISTINCT FROM o.status
OR s.has_successful_payment IS DISTINCT FROM EXISTS (
 SELECT 1 FROM public.payments p WHERE p.order_id=o.order_id AND p.status='SUCCESS'
);
