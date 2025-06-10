/*==================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)

This stored procedure performs the ETL process to populate the silver schema tables from bronze schema.
Actions performed:
- Changes silver table structure: dropping existing table and recreating it with correct column names and data types
- Truncates silver tables
- Inserts transformed and cleansed data from bronze into silver tables.

Parameters: none

Usage example: EXEC silver.load_silver;
==================================================*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================';
		PRINT 'Loading Silver Layer';
		PRINT '======================================';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname, -- Remove unwanted spaces
			TRIM(cst_lastname) cst_lastname, -- Remove unwanted spaces
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a' -- Handle missing values 
			END cst_marital_status, -- Normalize marital status values to readable format
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a' -- Handle missing values 
			END cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) t -- Remove duplicates
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Updating Table Format: silver.crm_prd_info';
		IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
			DROP TABLE silver.crm_prd_info;
		CREATE TABLE silver.crm_prd_info (
			prd_id			INT,
			cat_id			NVARCHAR(50),
			prd_key			NVARCHAR(50),
			prd_name		NVARCHAR(50),
			prd_cost		INT,
			prd_line		NVARCHAR(50),
			prd_start_date	DATE,
			prd_end_date	DATE,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_date,
			prd_end_date
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key, -- Extract product key
			prd_name,
			ISNULL(prd_cost, 0) prd_cost, -- Handle missing information (Null -> 0)
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a' -- Handle missing data
			END prd_line, -- Map product line codes to descriptice values
			CAST(prd_start_date AS DATE) prd_start_date, -- Cast DATETIME to DATE
			CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) - 1 
			AS DATE) prd_end_date -- Cast DATETIME to DATE & Calculate end date as one day (-1) before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Updating Table Format: silver.crm_sls_details';
		IF OBJECT_ID ('silver.crm_sls_details', 'U') IS NOT NULL
			DROP TABLE silver.crm_sls_details; 
		CREATE TABLE silver.crm_sls_details (
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id INT,
			sls_order_date DATE,
			sls_ship_date DATE,
			sls_due_date DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
		PRINT '>> Truncating Table: silver.crm_sls_details';
		TRUNCATE TABLE silver.crm_sls_details;
		PRINT '>> Inserting Data Into: silver.crm_sls_details';
		INSERT INTO silver.crm_sls_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_date,
			sls_ship_date,
			sls_due_date,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE) 
			END sls_order_date, -- Handle invalid data and Cast INT to DATE
			CASE WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE) 
			END sls_ship_date, -- Handle invalid data and Cast INT to DATE
			CASE WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE) 
			END sls_due_date, -- Handle invalid data and Cast INT to DATE
			CASE WHEN sls_sales IS NULL or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END sls_sales, -- Recalculate sales of original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL or sls_price <= 0
					THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END sls_price -- Derive price of original value is missing or incorrect
		FROM bronze.crm_sls_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';

		PRINT '--------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				 ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL 
				 ELSE bdate 
			END bdate, -- Set future birtdates to NULL
			CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a'
			END gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid, '-', '') cid, -- Match the writing format
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END cntry -- Normalize and handle missing or blank values
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 
			(id,
			cat,
			subcat,
			maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM
		bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';		
		SET @batch_end_time = GETDATE();
			PRINT '==================================';
			PRINT 'Loading Layer is Completed';
			PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '==================================';
	END TRY
	BEGIN CATCH
		PRINT '==================================';
		PRINT 'Error Occured';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================';
	END CATCH
END;

-- to run the stored procedure: EXEC silver.load_silver;
