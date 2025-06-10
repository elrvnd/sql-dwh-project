/*========================================================
Data Quality Checks

This script performs various quality checks for data consistency, accuracy, and standardization
across the silver schema. It includes checks for:
- Nulls or duplicate primary key
- Unwanted spaces in string fields
- Data standardization and consistency
- Invalid data ranges and orders
- Data consistency between related fields.

Usage: 
run these checks one by one after loading the cleansed and transformed data into the silver layer, 
then investigate any discrepancies.
=========================================================*/


-- =============================================
-- Checking Table: silver.crm_cust_info
-- =============================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces in string values
-- Expectation: No Result
SELECT 
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT 
cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT 
cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT 
cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- Check Overall table
SELECT *
FROM silver.crm_cust_info;


-- =============================================
-- Checking Table: silver.crm_prd_info
-- =============================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Result
SELECT prd_name
FROM silver.crm_prd_info
WHERE prd_name != TRIM(prd_name);

-- Check for Nulls or Negative Numbers
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders
-- >> End date must not be earlier than start date
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date;

-- Check Overall Table
SELECT *
FROM silver.crm_prd_info;


-- =============================================
-- Checking Table: silver.crm_sls_details
-- =============================================
-- Check for Unwanted Spaces on String Type Data
SELECT 
*
FROM silver.crm_sls_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check for Invalid Dates
-- >> Order date must be earlier than the shipping date or due date
SELECT
*
FROM silver.crm_sls_details
WHERE sls_order_date > sls_ship_date OR sls_order_date > sls_due_date;

-- Check Data Consistency: Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

-- >> Rules in this scenario:
-- >> If sales is negative, zero, or null, derive it using quantity and price
-- >> If price is zero or null, calculate it using sales and quantity
-- >> If price is negative, convert it to a positive value
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sls_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 or sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Check Overall Table
SELECT * FROM silver.crm_sls_details;


-- =============================================
-- Checking Table: silver.erp_cust_az12
-- =============================================
-- Indentify Out-of-Range Dates
-- >> Check for birthdays in the future
SELECT DISTINCT
bdate FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Check All
SELECT *
FROM silver.erp_cust_az12;


-- =============================================
-- Checking Table: silver.erp_loc_a101
-- =============================================
 -- Data Standardization & Consistency
 SELECT DISTINCT cntry
 FROM silver.erp_loc_a101
 ORDER BY cntry;
 
 -- Check Overall Table
 SELECT *
 FROM silver.erp_loc_a101;


-- =============================================
-- Checking Table: silver.erp_cust_az12
-- =============================================
-- Check for Unwanted Spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization and Consistency
SELECT DISTINCT
cat 
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
subcat 
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance 
FROM silver.erp_px_cat_g1v2;

-- Check Overall Table
SELECT *
FROM silver.erp_px_cat_g1v2;





