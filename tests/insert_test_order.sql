-- Inserts exactly one fully populated order and prints its new order_id.
WITH customer_choice AS (
  SELECT customer_id FROM public.customers ORDER BY customer_id LIMIT 1
), product_choice AS (
  SELECT product_id, price FROM public.products ORDER BY product_id LIMIT 1
), new_order AS (
  INSERT INTO public.orders(customer_id, status, created_at)
  SELECT customer_id, 'COMPLETED', NOW() FROM customer_choice
  RETURNING order_id
), new_item AS (
  INSERT INTO public.order_items(order_id, product_id, quantity, unit_price)
  SELECT n.order_id, p.product_id, 2, p.price
  FROM new_order n CROSS JOIN product_choice p
  RETURNING order_id, quantity, unit_price
), new_payment AS (
  INSERT INTO public.payments(order_id, amount, status, created_at)
  SELECT order_id, quantity * unit_price, 'SUCCESS', NOW()
  FROM new_item
  RETURNING order_id
)
SELECT order_id AS inserted_order_id FROM new_payment;
