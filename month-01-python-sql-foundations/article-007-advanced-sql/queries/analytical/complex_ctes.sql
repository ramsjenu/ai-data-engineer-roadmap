-- ============================================================================
-- COMPLEX CTEs FOR DATA ENGINEERING
-- ============================================================================

-- 1. Multi-level CTE - Customer Cohort Analysis
-- Use case: Analyze customer retention by registration cohort
-- ============================================================================

WITH customer_cohorts AS (
    -- Step 1: Assign each customer to a cohort (month they registered)
    SELECT 
        customer_id,
        DATE_TRUNC('month', registration_date) AS cohort_month
    FROM customers
),
customer_orders AS (
    -- Step 2: Get all orders with cohort information
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', o.order_date) AS order_month,
        o.total_amount
    FROM customer_cohorts cc
    JOIN orders o ON cc.customer_id = o.customer_id
    WHERE o.order_status = 'Delivered'
),
cohort_metrics AS (
    -- Step 3: Calculate metrics per cohort per month
    SELECT 
        cohort_month,
        order_month,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(total_amount) AS revenue,
        -- Calculate months since cohort start
        EXTRACT(YEAR FROM AGE(order_month, cohort_month)) * 12 + 
        EXTRACT(MONTH FROM AGE(order_month, cohort_month)) AS months_since_cohort
    FROM customer_orders
    GROUP BY cohort_month, order_month
),
cohort_sizes AS (
    -- Step 4: Get initial cohort sizes
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
-- Step 5: Calculate retention percentages
SELECT 
    cm.cohort_month,
    cm.months_since_cohort,
    cm.active_customers,
    cs.cohort_size,
    ROUND((cm.active_customers::NUMERIC / cs.cohort_size * 100), 2) AS retention_rate,
    cm.revenue,
    ROUND(cm.revenue / cm.active_customers, 2) AS revenue_per_customer
FROM cohort_metrics cm
JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
ORDER BY cm.cohort_month, cm.months_since_cohort;

-- 💡 Explanation:
-- This is a 5-step cohort analysis - the gold standard for SaaS/subscription businesses
-- Step 1: Group customers by when they signed up (cohort_month)
-- Step 2: Track all their orders over time
-- Step 3: Calculate how many customers from each cohort are active each month
-- Step 4: Get the starting size of each cohort
-- Step 5: Calculate retention rate (what % of original cohort is still active)
-- This shows if your product has good retention or if customers churn quickly


-- 2. Recursive CTE - Category Hierarchy
WITH RECURSIVE category_tree AS (
    -- Base case: top-level categories (no parent)
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        category_name::TEXT AS full_path,
        0 AS level
    FROM categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    -- Recursive case: subcategories
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        (ct.full_path || ' > ' || c.category_name)::TEXT AS full_path,
        ct.level + 1 AS level
    FROM categories c
    JOIN category_tree ct ON c.parent_category_id = ct.category_id
)
SELECT 
    category_id,
    REPEAT('  ', level) || category_name AS indented_name,
    full_path,
    level
FROM category_tree
ORDER BY full_path;

-- 💡 Explanation:
-- Recursive CTEs handle hierarchical data (org charts, category trees, bill of materials)
-- Base case: Start with top-level items (no parent)
-- Recursive case: Find children of items we've already found
-- PostgreSQL keeps recursing until no new rows are found
-- The REPEAT('  ', level) indents subcategories visually
-- This is how e-commerce sites build "Electronics > Laptops > Gaming Laptops" breadcrumbs


-- 3. CTE with Window Functions - Customer Lifetime Value
-- Use case: Calculate CLV and identify high-value customers
-- ============================================================================

WITH customer_metrics AS (
    -- Calculate per-customer metrics
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.registration_date,
        c.customer_segment,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value,
        MAX(o.order_date) AS last_order_date,
        MIN(o.order_date) AS first_order_date,
        CURRENT_DATE - MAX(o.order_date) AS days_since_last_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id 
        AND o.order_status = 'Delivered'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.registration_date, c.customer_segment
),
customer_lifetime_value AS (
    -- Calculate CLV and customer health scores
    SELECT 
        *,
        -- Simple CLV: total revenue * (1 + expected future orders)
        total_revenue * (1 + (total_orders / NULLIF(EXTRACT(MONTH FROM AGE(last_order_date, first_order_date)), 0))) AS estimated_clv,
        -- Recency score (lower is better)
        CASE 
            WHEN days_since_last_order <= 30 THEN 5
            WHEN days_since_last_order <= 60 THEN 4
            WHEN days_since_last_order <= 90 THEN 3
            WHEN days_since_last_order <= 180 THEN 2
            ELSE 1
        END AS recency_score,
        -- Frequency score
        CASE 
            WHEN total_orders >= 10 THEN 5
            WHEN total_orders >= 7 THEN 4
            WHEN total_orders >= 5 THEN 3
            WHEN total_orders >= 3 THEN 2
            ELSE 1
        END AS frequency_score,
        -- Monetary score
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS monetary_score
    FROM customer_metrics
    WHERE total_orders > 0
)
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value,
    days_since_last_order,
    ROUND(estimated_clv::numeric, 2) AS estimated_clv,
    recency_score,
    frequency_score,
    monetary_score,
    recency_score + frequency_score + monetary_score AS rfm_score,
    CASE 
        WHEN recency_score + frequency_score + monetary_score >= 13 THEN 'Champions'
        WHEN recency_score + frequency_score + monetary_score >= 10 THEN 'Loyal'
        WHEN recency_score + frequency_score + monetary_score >= 7 THEN 'Potential'
        ELSE 'At Risk'
    END AS customer_category
FROM customer_lifetime_value
ORDER BY rfm_score DESC, estimated_clv DESC;

-- 💡 Explanation:
-- This is RFM (Recency, Frequency, Monetary) analysis - the foundation of customer segmentation
-- Recency: How recently did they buy? (recent buyers are more likely to buy again)
-- Frequency: How often do they buy? (frequent buyers are loyal)
-- Monetary: How much do they spend? (high spenders are valuable)
-- Each dimension gets a score 1-5, total RFM score 3-15
-- Champions (13-15): Your best customers - give them VIP treatment
-- Loyal (10-12): Regular customers - upsell opportunities
-- Potential (7-9): Occasional buyers - nurture them
-- At Risk (<7): Inactive or low-value - re-engagement campaigns
-- The estimated_clv calculation projects future value based on past behavior


-- 4. CTE for Data Quality Checks
-- Use case: Identify data quality issues before analysis
-- ============================================================================

WITH data_quality_checks AS (
    -- Check 1: Orders without items
    SELECT 
        'Orders without items' AS issue_type,
        COUNT(*) AS issue_count
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    WHERE oi.order_item_id IS NULL
    
    UNION ALL
    
    -- Check 2: Negative prices
    SELECT 
        'Products with negative prices' AS issue_type,
        COUNT(*) AS issue_count
    FROM products
    WHERE price < 0
    
    UNION ALL
    
    -- Check 3: Orders with mismatched totals
    SELECT 
        'Orders with incorrect totals' AS issue_type,
        COUNT(*) AS issue_count
    FROM orders o
    JOIN (
        SELECT order_id, SUM(line_total) AS calculated_total
        FROM order_items
        GROUP BY order_id
    ) oi ON o.order_id = oi.order_id
    WHERE ABS(o.total_amount - oi.calculated_total) > 0.01
    
    UNION ALL
    
    -- Check 4: Customers without orders
    SELECT 
        'Customers with no orders' AS issue_type,
        COUNT(*) AS issue_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_id IS NULL
    
    UNION ALL
    
    -- Check 5: Future-dated orders
    SELECT 
        'Orders with future dates' AS issue_type,
        COUNT(*) AS issue_count
    FROM orders
    WHERE order_date > CURRENT_DATE
)
SELECT 
    issue_type,
    issue_count,
    CASE 
        WHEN issue_count = 0 THEN '✅ PASS'
        WHEN issue_count < 10 THEN '⚠️  WARNING'
        ELSE '❌ FAIL'
    END AS status
FROM data_quality_checks
ORDER BY issue_count DESC;

-- 💡 Explanation:
-- Data quality checks should run BEFORE your analysis pipeline
-- This CTE runs 5 common checks and flags issues
-- Orders without items = broken foreign keys or incomplete data entry
-- Negative prices = data entry errors or system bugs
-- Mismatched totals = calculation errors (order total ≠ sum of line items)
-- Customers without orders = inactive users or test accounts
-- Future dates = timezone issues or system clock problems
-- In production, these checks trigger alerts or block the pipeline
-- Better to catch bad data early than produce wrong reports!