SELECT c.customer_id,
       c.first_name || ' ' || c.last_name AS customer_name,
       SUM(o.total_amount) AS total_spent,
       RANK() OVER (ORDER BY SUM(o.total_amount) DESC) AS spend_rank
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'Delivered'
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 10;

SELECT order_date,
       SUM(total_amount) OVER (ORDER BY order_date) AS cumulative_revenue
FROM orders
WHERE order_status='Delivered'
ORDER BY order_date;

-- Calculate Avg Order Value per City, keeping individual orders
SELECT shipping_city,
       order_id,
       total_amount,
       AVG(total_amount) OVER (PARTITION BY shipping_city) AS city_avg
FROM orders
WHERE order_status='Delivered'
ORDER BY shipping_city;