-- ============================================================================
-- ADVANCED WINDOW FUNCTIONS FOR DATA ENGINEERING
-- ============================================================================

-- 1. LAG and LEAD - Compare with previous/next rows
-- Use case: Calculate month-over-month growth
-- ============================================================================

WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount) AS revenue
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    revenue - LAG(revenue) OVER (ORDER BY month) AS revenue_change,
    ROUND(
        ((revenue - LAG(revenue) OVER (ORDER BY month)) / 
         LAG(revenue) OVER (ORDER BY month) * 100), 2
    ) AS growth_percentage
FROM monthly_revenue
ORDER BY month;

-- 💡 Explanation:
-- LAG() fetches the previous row's value within the window
-- This calculates month-over-month revenue growth - critical for business reporting
-- The OVER (ORDER BY month) defines the window ordering
-- We calculate both absolute change and percentage growth


-- 2. FIRST_VALUE and LAST_VALUE - Get boundary values
-- Use case: Compare each customer's order to their first order
-- ============================================================================

SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_id,
    o.order_date,
    o.total_amount,
    FIRST_VALUE(o.total_amount) OVER (
        PARTITION BY c.customer_id 
        ORDER BY o.order_date
    ) AS first_order_amount,
    o.total_amount - FIRST_VALUE(o.total_amount) OVER (
        PARTITION BY c.customer_id 
        ORDER BY o.order_date
    ) AS amount_vs_first_order
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'Delivered'
ORDER BY c.customer_id, o.order_date
LIMIT 20;

-- 💡 Explanation:
-- FIRST_VALUE() gets the first value in each partition (customer)
-- PARTITION BY creates separate windows for each customer
-- This shows if customers are spending more or less than their first purchase
-- Essential for customer lifetime value (CLV) analysis


-- 3. NTILE - Divide data into buckets
-- Use case: Customer segmentation by spend
-- ============================================================================

WITH customer_spend AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.city,
        SUM(o.total_amount) AS total_spent,
        COUNT(o.order_id) AS order_count
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'Delivered'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city
)
SELECT 
    customer_id,
    customer_name,
    city,
    total_spent,
    order_count,
    NTILE(4) OVER (ORDER BY total_spent DESC) AS spend_quartile,
    CASE 
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 1 THEN 'VIP'
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 2 THEN 'High Value'
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 3 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM customer_spend
ORDER BY total_spent DESC;

-- 💡 Explanation:
-- NTILE(4) divides customers into 4 equal groups (quartiles)
-- Top 25% become VIP, next 25% High Value, etc.
-- This is how companies do RFM (Recency, Frequency, Monetary) segmentation
-- The CASE statement translates quartiles into business-friendly labels


-- 4. PERCENT_RANK - Calculate percentile ranking
-- Use case: Find top 10% customers by revenue
-- ============================================================================

WITH customer_revenue AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(o.total_amount) AS total_revenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'Delivered'
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    customer_id,
    customer_name,
    total_revenue,
    PERCENT_RANK() OVER (ORDER BY total_revenue DESC) AS percentile_rank,
    CASE 
        WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.10 THEN 'Top 10%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.25 THEN 'Top 25%'
        WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS revenue_tier
FROM customer_revenue
ORDER BY total_revenue DESC;

-- 💡 Explanation:
-- PERCENT_RANK() returns a value between 0 and 1 representing position
-- 0 = highest value, 1 = lowest value
-- This identifies your top performers for targeted campaigns
-- More precise than NTILE when you need exact percentiles


-- 5. ROW_NUMBER vs RANK vs DENSE_RANK
-- Use case: Understanding ranking differences
-- ============================================================================

WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.line_total) AS revenue
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Delivered'
    GROUP BY p.product_id, p.product_name
)
SELECT 
    product_name,
    revenue,
    ROW_NUMBER() OVER (ORDER BY revenue DESC) AS row_num,
    RANK() OVER (ORDER BY revenue DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rank
FROM product_sales
ORDER BY revenue DESC;

-- 💡 Explanation:
-- ROW_NUMBER: Always unique (1,2,3,4,5...)
-- RANK: Ties get same rank, next rank skips (1,2,2,4,5...)
-- DENSE_RANK: Ties get same rank, next rank continues (1,2,2,3,4...)
-- Use ROW_NUMBER for pagination, RANK for competitions, DENSE_RANK for categories


-- 6. Running Totals and Moving Averages
-- Use case: Cumulative revenue and 7-day moving average
-- ============================================================================

WITH daily_revenue AS (
    SELECT 
        order_date,
        SUM(total_amount) AS daily_revenue
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY order_date
)
SELECT 
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    daily_revenue - AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS deviation_from_avg
FROM daily_revenue
ORDER BY order_date;

-- 💡 Explanation:
-- ROWS BETWEEN defines the window frame
-- UNBOUNDED PRECEDING = from the start
-- 6 PRECEDING = last 7 days (including current)
-- Running totals show growth trajectory
-- Moving averages smooth out daily fluctuations
-- Deviation shows if today is above/below trend