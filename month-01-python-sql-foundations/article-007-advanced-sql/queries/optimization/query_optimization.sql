-- ============================================================================
-- QUERY OPTIMIZATION FOR DATA ENGINEERING
-- ============================================================================

-- 1. Understanding EXPLAIN ANALYZE
-- Use case: Diagnose slow queries
-- ============================================================================

-- Slow query (without optimization)
EXPLAIN ANALYZE
SELECT 
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'Delivered'
  AND o.order_date >= '2024-01-01'
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC;

-- 💡 Explanation:
-- EXPLAIN ANALYZE shows the query execution plan AND actually runs the query
-- Look for these red flags:
-- 1. "Seq Scan" on large tables = reading every row (SLOW!)
-- 2. High "cost" numbers = expensive operations
-- 3. "actual time" much higher than "estimated" = bad statistics
-- 4. "Nested Loop" with large datasets = consider hash join instead
-- The output shows execution time, rows processed, and which indexes were used


-- 2. Index Usage Analysis
-- Use case: Verify indexes are being used
-- ============================================================================

-- Check existing indexes
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Check index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- 💡 Explanation:
-- pg_indexes shows all indexes in your database
-- pg_stat_user_indexes shows how often each index is used
-- If idx_scan = 0, the index is never used (consider dropping it)
-- If a query is slow and idx_scan is low, you might need a better index
-- Unused indexes waste space and slow down INSERT/UPDATE operations


-- 3. Creating Optimal Indexes
-- Use case: Speed up common queries
-- ============================================================================

-- Composite index for common filter combinations
CREATE INDEX IF NOT EXISTS idx_orders_status_date 
ON orders(order_status, order_date);

-- Partial index (only index rows that match a condition)
CREATE INDEX IF NOT EXISTS idx_orders_delivered 
ON orders(order_date) 
WHERE order_status = 'Delivered';

-- Index on expression (for case-insensitive searches)
CREATE INDEX IF NOT EXISTS idx_customers_email_lower 
ON customers(LOWER(email));

-- Covering index (includes extra columns to avoid table lookup)
CREATE INDEX IF NOT EXISTS idx_orders_customer_covering 
ON orders(customer_id) 
INCLUDE (order_date, total_amount, order_status);

-- 💡 Explanation:
-- Composite indexes work for queries filtering on BOTH columns
-- Partial indexes are smaller and faster when you always filter the same way
-- Expression indexes enable fast searches on computed values (LOWER, UPPER, etc.)
-- Covering indexes include extra columns so PostgreSQL doesn't need to look up the table
-- Rule of thumb: Index columns used in WHERE, JOIN, and ORDER BY clauses


-- 4. Query Rewriting for Performance
-- Use case: Transform slow queries into fast ones
-- ============================================================================

-- SLOW: Correlated subquery (runs once per row)
EXPLAIN ANALYZE
SELECT 
    c.customer_id,
    c.first_name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count
FROM customers c;

-- FAST: JOIN with aggregation (runs once total)
EXPLAIN ANALYZE
SELECT 
    c.customer_id,
    c.first_name,
    COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name;

-- 💡 Explanation:
-- Correlated subqueries execute once per outer row (100 customers = 100 executions)
-- JOINs with GROUP BY execute once total (1 execution)
-- This can be 100x faster on large datasets
-- Always prefer JOINs over correlated subqueries when possible


-- 5. Using CTEs vs Subqueries
-- Use case: When to use which
-- ============================================================================

-- CTE (materialized - computed once)
EXPLAIN ANALYZE
WITH high_value_customers AS (
    SELECT customer_id, SUM(total_amount) AS total_spent
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY customer_id
    HAVING SUM(total_amount) > 100000
)
SELECT c.first_name, c.last_name, hvc.total_spent
FROM high_value_customers hvc
JOIN customers c ON hvc.customer_id = c.customer_id;

-- Subquery (may be optimized differently)
EXPLAIN ANALYZE
SELECT c.first_name, c.last_name, sub.total_spent
FROM customers c
JOIN (
    SELECT customer_id, SUM(total_amount) AS total_spent
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY customer_id
    HAVING SUM(total_amount) > 100000
) sub ON c.customer_id = sub.customer_id;

-- 💡 Explanation:
-- In PostgreSQL 12+, CTEs are optimized similarly to subqueries
-- Use CTEs for readability and when you reference the same data multiple times
-- Use subqueries when the optimizer can push down filters
-- Check EXPLAIN ANALYZE to see which performs better for your specific query


-- 6. Avoiding SELECT *
-- Use case: Reduce data transfer and memory usage
-- ============================================================================

-- BAD: Fetches all columns (slow, uses more memory)
EXPLAIN ANALYZE
SELECT * 
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01';

-- GOOD: Fetches only needed columns (fast, efficient)
EXPLAIN ANALYZE
SELECT 
    o.order_id,
    o.order_date,
    o.total_amount,
    c.first_name,
    c.last_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01';

-- 💡 Explanation:
-- SELECT * fetches all columns, even ones you don't need
-- This increases I/O, network transfer, and memory usage
-- Covering indexes can't help if you select all columns
-- Always specify only the columns you actually need
-- In production, SELECT * can make queries 2-10x slower


-- 7. Partitioning Large Tables
-- Use case: Speed up queries on time-series data
-- ============================================================================

-- Create partitioned table (PostgreSQL 10+)
CREATE TABLE orders_partitioned (
    order_id SERIAL,
    customer_id INTEGER,
    order_date DATE NOT NULL,
    order_status VARCHAR(20),
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (order_date);

-- Create partitions for each quarter
CREATE TABLE orders_2024_q1 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE orders_2024_q2 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

CREATE TABLE orders_2024_q3 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');

CREATE TABLE orders_2024_q4 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

-- Query automatically uses only relevant partitions
EXPLAIN ANALYZE
SELECT * FROM orders_partitioned
WHERE order_date BETWEEN '2024-07-01' AND '2024-09-30';

-- 💡 Explanation:
-- Partitioning splits large tables into smaller physical tables
-- Queries only scan relevant partitions (partition pruning)
-- Perfect for time-series data (orders, logs, events)
-- Each partition can have its own indexes
-- Old partitions can be archived or dropped easily
-- Can improve query performance 10-100x on large tables


-- 8. Analyzing Query Performance
-- Use case: Identify bottlenecks
-- ============================================================================

-- Update table statistics (run after large data changes)
ANALYZE customers;
ANALYZE orders;
ANALYZE order_items;
ANALYZE products;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Find slow queries (requires pg_stat_statements extension)
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT 
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- 💡 Explanation:
-- ANALYZE updates statistics that the query planner uses
-- Run it after bulk inserts, updates, or deletes
-- pg_size_pretty shows human-readable sizes (MB, GB)
-- Large index_size relative to table_size might indicate too many indexes
-- pg_stat_statements tracks all queries and their performance
-- Use it to find your slowest queries in production