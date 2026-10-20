-- All violation counts should be zero after sync + successful warehouse load.
SELECT 'missing_facts' AS check_name, COUNT(*) AS violations
FROM public.order_items oi
JOIN staging.orders s ON s.order_id = oi.order_id
LEFT JOIN warehouse.fact_order_items f ON f.order_item_id = oi.order_item_id
WHERE f.order_item_id IS NULL
UNION ALL
SELECT 'orphan_facts', COUNT(*)
FROM warehouse.fact_order_items f
LEFT JOIN public.order_items oi ON oi.order_item_id = f.order_item_id
WHERE oi.order_item_id IS NULL
UNION ALL
SELECT 'incorrect_line_amount', COUNT(*)
FROM warehouse.fact_order_items f
JOIN public.order_items oi ON oi.order_item_id = f.order_item_id
WHERE f.line_amount <> oi.quantity * oi.unit_price
UNION ALL
SELECT 'mismatched_order_status', COUNT(*)
FROM warehouse.fact_order_items f
JOIN staging.orders s ON s.order_id = f.order_id
WHERE f.order_status IS DISTINCT FROM s.order_status
   OR f.has_successful_payment IS DISTINCT FROM s.has_successful_payment;
