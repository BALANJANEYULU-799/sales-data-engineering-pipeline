-- ============================================================
-- sql/analytics.sql
-- Beginner-friendly SQL analytics queries with comments.
-- Run against the sales_pipeline.db SQLite database.
--
-- Usage (Windows):
--   sqlite3 data/sales_pipeline.db < sql/analytics.sql
--
-- Each query demonstrates a specific SQL concept.
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. BASIC SELECT, WHERE, ORDER BY
--    Concept: retrieve, filter, and sort rows
-- ────────────────────────────────────────────────────────────
-- List all completed orders above $500, newest first
SELECT
    order_id,
    product,
    region,
    net_amount,
    order_date
FROM orders_clean
WHERE status = 'Completed'
  AND net_amount > 500
ORDER BY order_date DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 2. AGGREGATE FUNCTIONS: COUNT, SUM, AVG, MIN, MAX
-- ────────────────────────────────────────────────────────────
-- Overall KPIs for completed orders
SELECT
    COUNT(*)                                    AS total_orders,
    ROUND(SUM(net_amount), 2)                   AS total_revenue,
    ROUND(AVG(net_amount), 2)                   AS avg_order_value,
    ROUND(MIN(net_amount), 2)                   AS min_order_value,
    ROUND(MAX(net_amount), 2)                   AS max_order_value,
    ROUND(SUM(discount_amount), 2)              AS total_discounts
FROM orders_clean
WHERE status = 'Completed';


-- ────────────────────────────────────────────────────────────
-- 3. GROUP BY and HAVING
--    Concept: aggregate per group; filter groups with HAVING
-- ────────────────────────────────────────────────────────────
-- Revenue by product category (only categories with > 5 orders)
SELECT
    category,
    COUNT(*)                    AS total_orders,
    ROUND(SUM(net_amount), 2)   AS total_revenue,
    ROUND(AVG(net_amount), 2)   AS avg_order_value,
    SUM(quantity)               AS total_units_sold
FROM orders_clean
WHERE status = 'Completed'
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 4. INNER JOIN (star schema demonstration)
--    Concept: combine rows from two tables on a matching key
-- ────────────────────────────────────────────────────────────
-- Join fact_sales with dim_product to get category info
SELECT
    fs.order_id,
    dp.product_name,
    dp.category,
    fs.net_amount,
    fs.status
FROM fact_sales fs
INNER JOIN dim_product dp ON fs.product_key = dp.product_key
WHERE fs.status = 'Completed'
ORDER BY fs.net_amount DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 5. LEFT JOIN
--    Concept: include all rows from the left table even if
--             no matching row exists in the right table
-- ────────────────────────────────────────────────────────────
-- Show all orders with their region name (LEFT JOIN handles NULLs gracefully)
SELECT
    fs.order_id,
    COALESCE(dr.region_name, 'Unknown') AS region,
    fs.net_amount
FROM fact_sales fs
LEFT JOIN dim_region dr ON fs.region_key = dr.region_key
ORDER BY fs.order_id
LIMIT 15;


-- ────────────────────────────────────────────────────────────
-- 6. CASE STATEMENT
--    Concept: conditional logic inside SQL (like if-else)
-- ────────────────────────────────────────────────────────────
-- Classify orders by order size
SELECT
    order_id,
    product,
    net_amount,
    CASE
        WHEN net_amount >= 1000 THEN 'Large Order'
        WHEN net_amount >= 200  THEN 'Medium Order'
        WHEN net_amount >= 50   THEN 'Small Order'
        ELSE 'Micro Order'
    END AS order_size_tier,
    CASE
        WHEN discount > 0 THEN 'Discounted'
        ELSE 'Full Price'
    END AS pricing_type
FROM orders_clean
WHERE status = 'Completed'
ORDER BY net_amount DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 7. SUBQUERY
--    Concept: a query nested inside another query
-- ────────────────────────────────────────────────────────────
-- Products whose average order value is above the overall average
SELECT
    product,
    category,
    ROUND(AVG(net_amount), 2) AS avg_order_value
FROM orders_clean
WHERE status = 'Completed'
GROUP BY product, category
HAVING AVG(net_amount) > (
    -- Subquery: compute the overall average for comparison
    SELECT AVG(net_amount)
    FROM orders_clean
    WHERE status = 'Completed'
)
ORDER BY avg_order_value DESC;


-- ────────────────────────────────────────────────────────────
-- 8. CTE (Common Table Expression)
--    Concept: a named temporary result set (WITH clause)
--    CTEs make complex queries easier to read and debug.
-- ────────────────────────────────────────────────────────────
-- Monthly revenue with running total using a CTE
WITH monthly_revenue AS (
    -- Step 1: calculate revenue per month
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(net_amount), 2) AS monthly_revenue,
        COUNT(*) AS total_orders
    FROM orders_clean
    WHERE status = 'Completed'
    GROUP BY order_year, order_month, order_month_name
)
-- Step 2: add running total window function
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    total_orders,
    ROUND(SUM(monthly_revenue) OVER (
        ORDER BY order_year, order_month
    ), 2) AS running_total
FROM monthly_revenue
ORDER BY order_year, order_month;


-- ────────────────────────────────────────────────────────────
-- 9. WINDOW FUNCTIONS: ROW_NUMBER and RANK
--    Concept: compute a value across a set of rows related
--             to the current row (PARTITION BY, ORDER BY)
-- ────────────────────────────────────────────────────────────
-- Rank products by revenue within each region
SELECT
    region,
    product,
    ROUND(SUM(net_amount), 2) AS revenue,
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY SUM(net_amount) DESC
    ) AS row_num,
    RANK() OVER (
        PARTITION BY region
        ORDER BY SUM(net_amount) DESC
    ) AS revenue_rank_in_region
FROM orders_clean
WHERE status = 'Completed'
GROUP BY region, product
ORDER BY region, revenue_rank_in_region;


-- ────────────────────────────────────────────────────────────
-- 10. MONTHLY SALES TREND
-- ────────────────────────────────────────────────────────────
SELECT
    order_year,
    order_month,
    order_month_name,
    COUNT(*)                   AS total_orders,
    ROUND(SUM(net_amount), 2)  AS monthly_revenue,
    ROUND(AVG(net_amount), 2)  AS avg_order_value,
    ROUND(SUM(discount_amount), 2) AS discounts_given
FROM orders_clean
WHERE status = 'Completed'
GROUP BY order_year, order_month, order_month_name
ORDER BY order_year, order_month;


-- ────────────────────────────────────────────────────────────
-- 11. TOP-SELLING PRODUCTS
-- ────────────────────────────────────────────────────────────
SELECT
    product,
    category,
    COUNT(*)                   AS total_orders,
    SUM(quantity)              AS units_sold,
    ROUND(SUM(net_amount), 2)  AS total_revenue,
    ROUND(AVG(net_amount), 2)  AS avg_order_value
FROM orders_clean
WHERE status = 'Completed'
GROUP BY product, category
ORDER BY total_revenue DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 12. REVENUE BY REGION
-- ────────────────────────────────────────────────────────────
SELECT
    region,
    COUNT(*)                   AS total_orders,
    ROUND(SUM(net_amount), 2)  AS total_revenue,
    ROUND(AVG(net_amount), 2)  AS avg_order_value
FROM orders_clean
WHERE status = 'Completed'
GROUP BY region
ORDER BY total_revenue DESC;
