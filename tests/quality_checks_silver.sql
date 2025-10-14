/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLS or Duplicates in Primary Key 
-- Expectation: No Results 
SELECT 
	cst_id ,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR  cst_id IS NULL ; 

-- Check for Unwanted Spaces 
-- Expectation: No Results 
SELECT 
	cst_key 
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key) ;

-- Data Standardziation & Consistency 
SELECT DISTINCT 
	cst_marital_status 
FROM bronze.crm_cust_info ; 

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================	
-- Check for NULLS or Duplicates in Primary Key 
-- Expectation: No Results 
SELECT 
	prd_id ,
	COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

-- Check for Unwanted Spaces 
-- Expectation: No Results 
SELECT 
	prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) ;

-- Check for NULLS or Negative Values in Cost 
-- Expecatation: No Results 
SELECT 
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL ;

-- Data Standardization & Consistency 
SELECT DISTINCT 
	prd_nm 
FROM silver.crm_prd_info ;

-- Check Invalid Date Orders (Start Date > End Date)
-- Expectations: No Results 
SELECT 
*
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt ;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
	NULLIF(sls_orders_dt , 0 ) as sls_orders_dt
FROM bronze.crm_sales_details
WHERE sls_orders_dt <= 0 
OR sls_orders_dt IS NULL 
OR LEN(sls_orders_dt) != 8 
OR sls_orders_dt < 19000101
OR sls_orders_dt > 20500101 ;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
	sls_orders_dt
FROM bronze.crm_sales_details
WHERE sls_orders_dt > sls_ship_dt 
OR	  sls_orders_dt > sls_due_dt ;

-- Check consistency: Sales = Quantity * Price 
-- Expectations: No Results 
SELECT 
	sls_sales ,
	sls_quantity ,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales <= 0
OR sls_price <= 0
OR sls_quantity <= 0
OR sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL 
ORDER BY sls_sales , sls_quantity , sls_price ;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT 
	bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
OR    bdate > GETDATE() ;

-- Data Standardization & Consistency 
SELECT DISTINCT 
	gen 
FROM bronze.erp_cust_az12 ;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency 
SELECT DISTINCT 
	cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry ;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for NULLS or Duplicates in Primary Key 
-- Expectation: No Results 
SELECT 
	id ,
	COUNT(*) 
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL ; 

-- Check for Unwanted Spaces 
-- Expecations: No Results 
SELECT 
* 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
	OR subcat != TRIM(subcat) 
	OR maintenance != TRIM(maintenance) ;

-- Data Stanardiztaion & Consistency 
SELECT DISTINCT 
	maintenance 
FROM bronze.erp_px_cat_g1v2 ;



