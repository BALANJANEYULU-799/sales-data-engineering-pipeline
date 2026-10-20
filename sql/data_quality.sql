-- ============================================================
-- sql/data_quality.sql
-- Data quality validation queries for the orders_clean table.
-- Run these AFTER the pipeline to verify data integrity.
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. Null counts per column
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                             AS total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)   AS null_order_id,
    SUM(CASE WHEN product IS NULL THEN 1 ELSE 0 END)    AS null_product,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END)   AS null_category,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END)   AS null_quantity,
    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
    SUM(CASE WHEN net_amount IS NULL THEN 1 ELSE 0 END) AS null_net_amount
FROM orders_clean;


-- ────────────────────────────────────────────────────────────
-- 2. Invalid values check
-- ────────────────────────────────────────────────────────────
SELECT
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END)    AS invalid_quantity,
    SUM(CASE WHEN unit_price <= 0 THEN 1 ELSE 0 END)  AS invalid_unit_price,
    SUM(CASE WHEN net_amount < 0 THEN 1 ELSE 0 END)   AS negative_net_amount,
    SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS invalid_discount
FROM orders_clean;


-- ────────────────────────────────────────────────────────────
-- 3. Duplicate order_id check
-- ────────────────────────────────────────────────────────────
SELECT
    order_id,
    COUNT(*) AS occurrences
FROM orders_clean
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
-- Expected result: 0 rows (all order_ids should be unique)


-- ────────────────────────────────────────────────────────────
-- 4. Status value distribution
-- ────────────────────────────────────────────────────────────
SELECT
    status,
    COUNT(*) AS count
FROM orders_clean
GROUP BY status
ORDER BY count DESC;
-- Expected: only Completed, Pending, Cancelled, Refunded


-- ────────────────────────────────────────────────────────────
-- 5. Net amount consistency check
--    net_amount should equal gross_amount - discount_amount
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(*) AS rows_with_inconsistent_net_amount
FROM orders_clean
WHERE ABS(net_amount - (gross_amount - discount_amount)) > 0.01;
-- Expected result: 0


-- ────────────────────────────────────────────────────────────
-- 6. Date range sanity check
-- ────────────────────────────────────────────────────────────
SELECT
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order,
    COUNT(*) AS total_orders
FROM orders_clean;
