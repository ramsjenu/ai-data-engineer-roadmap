-- ============================================================================
-- MATERIALIZED VIEWS FOR DATA ENGINEERING
-- ============================================================================

-- 1. Customer Summary Materialized View
-- Use case: Pre-compute expensive customer metrics
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.state,
    c.customer_segment,
    c.registration_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    MIN(o.order_date) AS first_order_date,
    CURRENT_DATE - MAX(o.order_date) AS days_since_last_order,
    -- RFM Scores
    CASE 
        WHEN CURRENT_DATE - MAX(o.order_date) <= 30 THEN 5
        WHEN CURRENT_DATE - MAX(o.order_date) <= 60 THEN 4
        WHEN CURRENT_DATE - MAX(o.order_date) <= 90 THEN 3
        WHEN CURRENT_DATE - MAX(o.order_date) <= 180 THEN 2
        ELSE 1
    END AS recency_score,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) >= 10 THEN 5
        WHEN COUNT(DISTINCT o.order_id) >= 7 THEN 4
        WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 3
        WHEN COUNT(DISTINCT o.order_id) >= 3 THEN 2
        ELSE 1
    END AS frequency_score,
    NTILE(5) OVER (ORDER BY SUM(o.total_amount) DESC) AS monetary_score
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id 
    AND o.order_status = 'Delivered'
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state, 
         c.customer_segment, c.registration_date;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_mv_customer_summary_segment 
ON mv_customer_summary(customer_segment);

CREATE INDEX IF NOT EXISTS idx_mv_customer_summary_city 
ON mv_customer_summary(city);

-- Query the materialized view (FAST!)
SELECT * FROM mv_customer_summary
WHERE customer_segment = 'Premium'
ORDER BY total_revenue DESC
LIMIT 10;

-- Refresh the materialized view (run daily/hourly)
REFRESH MATERIALIZED VIEW mv_customer_summary;

-- 💡 Explanation:
-- Materialized views store query results physically on disk
-- The complex aggregation runs once, then queries are instant
-- Perfect for dashboards that show the same metrics repeatedly
-- Refresh periodically (hourly, daily) to update the data
-- Trade-off: Data is slightly stale but queries are 100x faster
-- Use for reports, dashboards, and analytics that don't need real-time data


-- 2. Product Performance Materialized View
-- Use case: Product analytics for business intelligence
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    cat.category_name,
    p.price,
    p.cost,
    p.price - p.cost AS profit_per_unit,
    ROUND(((p.price - p.cost) / p.price * 100), 2) AS profit_margin_pct,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.line_total) AS total_revenue,
    SUM(oi.quantity * p.cost) AS total_cost,
    SUM(oi.line_total) - SUM(oi.quantity * p.cost) AS total_profit,
    AVG(oi.discount_percent) AS avg_discount_pct,
    -- Ranking
    RANK() OVER (ORDER BY SUM(oi.line_total) DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY cat.category_name ORDER BY SUM(oi.line_total) DESC) AS category_rank
FROM products p
JOIN categories cat ON p.category_id = cat.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id 
    AND o.order_status = 'Delivered'
GROUP BY p.product_id, p.product_name, cat.category_name, p.price, p.cost;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_mv_product_performance_category 
ON mv_product_performance(category_name);

-- Query examples
-- Top 10 products by revenue
SELECT product_name, category_name, total_revenue, revenue_rank
FROM mv_product_performance
ORDER BY revenue_rank
LIMIT 10;

-- Most profitable products
SELECT product_name, total_profit, profit_margin_pct
FROM mv_product_performance
ORDER BY total_profit DESC
LIMIT 10;

-- 💡 Explanation:
-- This view pre-calculates all product metrics
-- Profit calculations (revenue - cost) are done once
-- Rankings are pre-computed for instant "top products" queries
-- Business users can query this view without understanding complex JOINs
-- Refresh nightly to get updated sales data


-- 3. Daily Revenue Trend Materialized View
-- Use case: Time-series analytics for dashboards
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_revenue AS
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS daily_revenue,
    AVG(total_amount) AS avg_order_value,
    -- Moving averages
    AVG(SUM(total_amount)) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    AVG(SUM(total_amount)) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30day,
    -- Cumulative metrics
    SUM(SUM(total_amount)) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM orders
WHERE order_status = 'Delivered'
GROUP BY order_date
ORDER BY order_date;

-- Query for dashboard
SELECT 
    order_date,
    daily_revenue,
    moving_avg_7day,
    daily_revenue - moving_avg_7day AS deviation_from_avg,
    cumulative_revenue
FROM mv_daily_revenue
WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY order_date DESC;

-- 💡 Explanation:
-- Time-series data is perfect for materialized views
-- Moving averages smooth out daily fluctuations
-- Cumulative revenue shows growth trajectory
-- Dashboards query this view for instant chart rendering
-- Refresh once per day (after midnight) to add new data


-- 4. Concurrent Refresh (No Locking)
-- Use case: Refresh without blocking queries
-- ============================================================================

-- Create with CONCURRENTLY option (requires unique index)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_customer_summary_pk 
ON mv_customer_summary(customer_id);

-- Refresh without locking (queries can still read old data)
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_summary;

-- 💡 Explanation:
-- Normal REFRESH locks the view (queries wait)
-- CONCURRENTLY allows queries to read old data while refreshing
-- Requires a unique index on the view
-- Takes longer but doesn't block users
-- Use for views that are queried frequently


-- 5. Automated Refresh with pg_cron
-- Use case: Schedule automatic refreshes
-- ============================================================================

-- Install pg_cron extension (run once)
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule daily refresh at 2 AM
-- SELECT cron.schedule('refresh-customer-summary', '0 2 * * *', 
--     'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_summary');

-- Schedule hourly refresh
-- SELECT cron.schedule('refresh-product-performance', '0 * * * *', 
--     'REFRESH MATERIALIZED VIEW mv_product_performance');

-- View scheduled jobs
-- SELECT * FROM cron.job;

-- 💡 Explanation:
-- pg_cron schedules SQL commands like Linux cron
-- '0 2 * * *' = every day at 2 AM
-- '0 * * * *' = every hour at minute 0
-- Automates materialized view refreshes
-- No need for external schedulers or cron jobs
-- Perfect for data warehouse maintenance