-- All customers even if they haven't placed any orders
SELECT c.customer_id, 
       c.first_name, 
       o.order_id, 
       o.order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
ORDER BY c.customer_id
LIMIT 10;

-- Customer orders with product details
SELECT c.first_name || ' ' || c.last_name AS customer_name,
       p.product_name,
       oi.quantity,
       oi.unit_price,
       (oi.quantity * oi.unit_price) AS gross_amount,
       o.order_date
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'Delivered'
ORDER BY o.order_date DESC
LIMIT 15;

