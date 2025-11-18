-- Customers with above-average total order value
SELECT first_name, last_name, email
FROM customers c
WHERE c.customer_id IN (
  SELECT customer_id 
  FROM orders 
  GROUP BY customer_id
  HAVING AVG(total_amount) > (
        SELECT AVG(total_amount) FROM orders
  )
);

WITH customer_spend AS (
  SELECT customer_id, SUM(total_amount) AS total_spent
  FROM orders
  WHERE order_status='Delivered'
  GROUP BY customer_id
),
ranked AS (
  SELECT customer_id, total_spent, 
         RANK() OVER (ORDER BY total_spent DESC) AS rank
  FROM customer_spend
)
SELECT c.first_name, c.last_name, r.total_spent, r.rank
FROM ranked r
JOIN customers c ON r.customer_id = c.customer_id
WHERE r.rank <= 10;