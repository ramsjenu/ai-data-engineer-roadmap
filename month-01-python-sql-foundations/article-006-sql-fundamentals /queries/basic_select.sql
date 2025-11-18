-- Retrieve all data from the customers table
SELECT * FROM customers;

-- Display only first 5 customers
SELECT customer_id, first_name, last_name, city 
FROM customers LIMIT 5;

-- Get customers from Pune
SELECT first_name, last_name, city 
FROM customers
WHERE city = 'Pune';

-- Get customers registered after July 2024
SELECT first_name, last_name, registration_date 
FROM customers
WHERE registration_date > '2024-07-01';

SELECT first_name, last_name, city, customer_segment
FROM customers
WHERE city IN ('Pune', 'Mumbai')
  AND customer_segment = 'Premium'
ORDER BY registration_date DESC;

-- Customers whose name starts with 'S'
SELECT first_name, last_name 
FROM customers 
WHERE first_name LIKE 'S%';

-- Case-insensitive search
SELECT first_name, last_name 
FROM customers 
WHERE first_name ILIKE 's%';

-- Sort by column ascending
SELECT product_name, price 
FROM products
ORDER BY price ASC;

-- Sort by price descending
SELECT product_name, price 
FROM products
ORDER BY price DESC
LIMIT 10;