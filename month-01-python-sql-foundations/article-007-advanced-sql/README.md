# Article 7: Advanced SQL for Data Engineering

## Overview
Production-level SQL techniques for data warehouses: analytical functions, CTEs, query optimization, materialized views, and data mart design.

## What's Included

### 1. Advanced Analytical Functions
**File:** `queries/analytical/window_functions_advanced.sql`

- **LAG/LEAD** - Compare with previous/next rows (MoM growth)
- **FIRST_VALUE/LAST_VALUE** - Boundary values in windows
- **NTILE** - Divide data into buckets (quartiles, deciles)
- **PERCENT_RANK** - Calculate percentile rankings
- **ROW_NUMBER vs RANK vs DENSE_RANK** - Understanding differences
- **Running Totals** - Cumulative metrics
- **Moving Averages** - Smooth time-series data

### 2. Complex CTEs
**File:** `queries/analytical/complex_ctes.sql`

- **Multi-level CTEs** - Customer cohort analysis
- **Recursive CTEs** - Hierarchical data (category trees)
- **CTEs with Window Functions** - Customer lifetime value (CLV)
- **RFM Analysis** - Recency, Frequency, Monetary segmentation
- **Data Quality Checks** - Automated validation

### 3. Query Optimization
**File:** `queries/optimization/query_optimization.sql`

- **EXPLAIN ANALYZE** - Diagnose slow queries
- **Index Strategies** - Composite, partial, covering indexes
- **Query Rewriting** - Transform slow queries to fast ones
- **CTEs vs Subqueries** - When to use which
- **Avoiding SELECT *** - Reduce data transfer
- **Table Partitioning** - Speed up time-series queries
- **Performance Monitoring** - pg_stat_statements

### 4. Materialized Views
**File:** `queries/views/materialized_views.sql`

- **Customer Summary View** - Pre-computed customer metrics
- **Product Performance View** - Product analytics
- **Daily Revenue Trend View** - Time-series analytics
- **Concurrent Refresh** - No-downtime updates
- **Automated Refresh** - pg_cron scheduling

### 5. Analytical Data Mart
**File:** `queries/warehouse/analytical_data_mart.sql`

- **Fact Table** - Order facts (star schema)
- **Dimension Tables** - Customer and product dimensions
- **Revenue Analysis** - By month and category
- **Cohort Analysis** - Customer retention
- **Product Affinity** - Recommendation engine foundation

## Setup

### Prerequisites
- PostgreSQL 12+ installed
- ecommerce_db database (from Article #6)
- Python 3.11+ (for automation scripts)

### Installation
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt