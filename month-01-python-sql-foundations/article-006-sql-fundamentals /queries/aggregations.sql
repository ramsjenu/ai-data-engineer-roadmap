-- How many customers per city?
SELECT city, COUNT(*) AS total_customers
FROM customers
GROUP BY city
ORDER BY total_customers DESC;

-- Revenue by payment method
SELECT payment_method, SUM(total_amount) AS total_revenue
FROM orders
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- Revenue and orders by state
SELECT shipping_state AS state,
       COUNT(order_id) AS total_orders,
       SUM(total_amount) AS total_revenue,
       ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
WHERE order_status = 'Delivered'
GROUP BY shipping_state
ORDER BY total_revenue DESC;

