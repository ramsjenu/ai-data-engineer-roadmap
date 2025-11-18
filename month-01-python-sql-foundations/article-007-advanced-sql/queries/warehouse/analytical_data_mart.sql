-- ============================================================================
-- ANALYTICAL DATA MART FOR BUSINESS INTELLIGENCE
-- ============================================================================

-- 1. Fact Table: Order Facts
-- Use case: Central fact table for star schema
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_orders AS
SELECT 
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(QUARTER FROM o.order_date) AS order_quarter,
    EXTRACT(MONTH FROM o.order_date) AS order_month,
    EXTRACT(DOW FROM o.order_date) AS order_day_of_week,
    o.order_status,
    o.payment_method,
    o.shipping_city,
    o.shipping_state,
    oi.quantity,
    oi.unit_price,
    oi.discount_percent,
    oi.line_total,
    p.cost AS unit_cost,
    oi.quantity * p.cost AS line_cost,
    oi.line_total - (oi.quantity * p.cost) AS line_profit
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'Delivered';

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON fact_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_year_month ON fact_orders(order_year, order_month);

-- 💡 Explanation:
-- Fact tables store measurable events (orders, sales, transactions)
-- Denormalized for query performance (includes dimensions inline)
-- Pre-calculated metrics (line_profit) avoid runtime calculations
-- Date dimensions extracted for easy time-based analysis
-- This is the foundation of a star schema data warehouse


-- 2. Dimension Table: Customer Dimension
-- Use case: Customer attributes for analysis
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_customers AS
SELECT 
    customer_id,
    first_name || ' ' || last_name AS customer_name,
    email,
    city,
    state,
    customer_segment,
    registration_date,
    EXTRACT(YEAR FROM registration_date) AS registration_year,
    DATE_PART('year', AGE(CURRENT_DATE, registration_date)) AS customer_age_years
FROM customers;

-- 💡 Explanation:
-- Dimension tables store descriptive attributes
-- Slowly changing dimensions (SCD) track historical changes
-- Customer segment, location, and tenure enable segmentation analysis


-- 3. Dimension Table: Product Dimension
-- Use case: Product attributes for analysis
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_products AS
SELECT 
    p.product_id,
    p.product_name,
    c.category_name,
    p.price,
    p.cost,
    p.price - p.cost AS profit_per_unit,
    ROUND(((p.price - p.cost) / p.price * 100), 2) AS profit_margin_pct,
    CASE 
        WHEN p.price < 1000 THEN 'Budget'
        WHEN p.price < 10000 THEN 'Mid-Range'
        WHEN p.price < 50000 THEN 'Premium'
        ELSE 'Luxury'
    END AS price_tier
FROM products p
JOIN categories c ON p.category_id = c.category_id;

-- 💡 Explanation:
-- Product dimensions enable product-level analysis
-- Price tiers created for segmentation
-- Profit margins pre-calculated for performance


-- 4. Analytical Query: Revenue by Month and Category
-- Use case: Business intelligence dashboard
-- ============================================================================

SELECT 
    f.order_year,
    f.order_month,
    dp.category_name,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS units_sold,
    SUM(f.line_total) AS revenue,
    SUM(f.line_profit) AS profit,
    ROUND(AVG(f.line_total), 2) AS avg_order_line_value
FROM fact_orders f
JOIN dim_products dp ON f.product_id = dp.product_id
GROUP BY f.order_year, f.order_month, dp.category_name
ORDER BY f.order_year, f.order_month, revenue DESC;

-- 💡 Explanation:
-- Star schema enables fast aggregations
-- JOINs are simple (fact to dimension)
-- Pre-calculated metrics (line_profit) avoid complex calculations
-- This query powers monthly revenue dashboards


-- 5. Analytical Query: Customer Cohort Analysis
-- Use case: Retention analysis
-- ============================================================================

WITH cohort_data AS (
    SELECT 
        dc.customer_id,
        DATE_TRUNC('month', dc.registration_date) AS cohort_month,
        DATE_TRUNC('month', f.order_date) AS order_month,
        f.line_total
    FROM dim_customers dc
    JOIN fact_orders f ON dc.customer_id = f.customer_id
)
SELECT 
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(line_total) AS revenue,
    EXTRACT(MONTH FROM AGE(order_month, cohort_month)) AS months_since_cohort
FROM cohort_data
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;

-- 💡 Explanation:
-- Cohort analysis tracks customer behavior over time
-- Shows retention rates by registration cohort
-- Essential for subscription and SaaS businesses


-- 6. Analytical Query: Product Affinity Analysis
-- Use case: "Customers who bought X also bought Y"
-- ============================================================================

WITH product_pairs AS (
    SELECT 
        f1.product_id AS product_a,
        f2.product_id AS product_b,
        COUNT(DISTINCT f1.order_id) AS co_occurrence_count
    FROM fact_orders f1
    JOIN fact_orders f2 ON f1.order_id = f2.order_id 
        AND f1.product_id < f2.product_id
    GROUP BY f1.product_id, f2.product_id
)
SELECT 
    dp1.product_name AS product_a_name,
    dp2.product_name AS product_b_name,
    pp.co_occurrence_count,
    RANK() OVER (PARTITION BY pp.product_a ORDER BY pp.co_occurrence_count DESC) AS affinity_rank
FROM product_pairs pp
JOIN dim_products dp1 ON pp.product_a = dp1.product_id
JOIN dim_products dp2 ON pp.product_b = dp2.product_id
WHERE pp.co_occurrence_count >= 3
ORDER BY pp.co_occurrence_count DESC
LIMIT 20;

-- 💡 Explanation:
-- Product affinity finds items frequently bought together
-- Powers "Customers also bought" recommendations
-- Self-join on order_id finds products in same order
-- Minimum threshold (>= 3) filters noise