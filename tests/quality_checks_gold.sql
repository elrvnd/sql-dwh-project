/* =================================================
Quality Check: Gold Layer

This script performs quality checks to validate integrity, consistency, and accuracy
of the gold layer. This ensures:
- Uniquenes of surrogate keys in dimension tables
- Referential integrity between fact and dimension tables
- Validation of relationships in the data model for analytical purposes.

Usage: run these checks after loading the gold layer, investigate any discrepancies found.
================================================== */

-- ===============================================
-- Check for gold.dim_customers
-- ===============================================
-- Check for Duplicates
-- Expectation: No Result
SELECT 
  customer_key,
  COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ===============================================
-- Check for gold.dim_products
-- ===============================================
-- Check duplicates
-- Expectation: No Result
SELECT 
  product_key, 
  COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ===============================================
-- Check for gold.fact_sales
-- ===============================================
-- Check the data model connectivity between fact and dimensions
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
