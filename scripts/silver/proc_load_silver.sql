/*
=========================================================
Stored Procedure: Load silver Layer (Bronze -> Silver) 
=========================================================
Script Purpose : 
	This stored procedure performs ETL ( Extract , Transform , Load ) process to
	build the 'silver' schena tables from the 'bronze' schema.
Action performed:
	- Truncates silver tables .
	- Inserts transformed and cleaned data from bronze to silver tables.

Usage Example: 
	EXEC silver.load_bronze
*/


CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	BEGIN TRY 
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME  , @batch_end_time DATETIME 
		SET @batch_start_time = GETDATE ()
		SET @start_time = GETDATE ()
		Print'==========================================';
		Print'          Loading Silver Layer            ';
		Print'==========================================';

		PRINT '---------------------------------------' ;
		Print '-->> Loading CRM Tables' ;
		PRINT '---------------------------------------' ;

		Print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info 
		Print '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id ,
			cst_key ,
			cst_firstname ,
			cst_lastname ,
			cst_marital_status ,
			cst_gndr ,
			cst_create_date 
		)
		SELECT
			cst_id ,
			cst_key ,
			TRIM(cst_firstname) as cst_firstname ,
			TRIM(cst_lastname) as cst_lastname  ,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 's' THEN 'Single'
				 ELSE 'n/a'
			END cst_marital_status ,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
				 ELSE 'n/a'
			END cst_gndr ,
			cst_create_date 
		FROM (SELECT * ,
			ROW_NUMBER () OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) Flag_Rank
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL ) t 
		WHERE Flag_Rank = 1
		SET @end_time = GETDATE () 
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ;
	 
		Print '-----------------------------------------';

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info
		Print '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt , 
			prd_end_dt 
		)
		SELECT 
			prd_id ,
			REPLACE(SUBSTRING(prd_key , 1 , 5 ) , '-' , '_') as cat_id , -- Derived column   
			SUBSTRING(prd_key ,  7 , LEN(prd_key) ) as prd_key  ,-- Derived column 
			prd_nm ,
			COALESCE(prd_cost , 0 ) as  prd_cost ,
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Raod' 
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'n/a'
			END prd_line , -- Maping Data 
			CAST(prd_start_dt as DATE) as prd_start_dt ,
			CAST(LEAD(prd_start_dt) OVER ( PARTITION BY prd_key ORDER BY prd_start_dt ) -1 as DATE)  as prd_end_dt 
		FROM bronze.crm_prd_info 
		SET @end_time = GETDATE ()
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ;

		Print '-----------------------------------------';

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: silver.crm_sales_datails';
		TRUNCATE TABLE silver.crm_sales_details
		Print '>> Inserting Data Into: silver.crm_sales_datails';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_orders_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price
		)
		SELECT 
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			CASE WHEN sls_orders_dt = 0 OR LEN(sls_orders_dt) != 8 THEN NULL 
				 ELSE CAST(CAST(sls_orders_dt as VARCHAR ) AS DATE)
			END sls_orders_dt , -- We make our dates more effective 
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
				 ELSE CAST(CAST(sls_ship_dt as VARCHAR ) AS DATE)
			END sls_ship_dt ,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
				 ELSE CAST(CAST(sls_due_dt as VARCHAR ) AS DATE)
			END sls_due_dt ,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price 
				 THEN sls_quantity * ABS(sls_price) 
				 ELSE sls_sales
			END sls_sales , -- Handling Nulls and Zeros 
			sls_quantity ,
			CASE WHEN sls_price <= 0 OR sls_price IS NULL 
				 THEN sls_sales / sls_quantity 
			ELSE ABS(sls_price) 
			END sls_price -- Handling Nulls and Zeros 
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE ()
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ; 


		PRINT '---------------------------------------' ;
		Print '-->> Loading ERP Tables' ;
		PRINT '---------------------------------------' ;

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		Print '>> Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 ( cid , bdate , gen)
		SELECT	
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid))
				ELSE cid
			END cid ,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate 
			END bdate ,
			CASE WHEN UPPER(TRIM(gen)) in ('F' , 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) in ('M' , 'MALE') THEN 'Male'
				 ELSE 'n/a' 
			END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE ()
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ;

		Print '-----------------------------------------';
		SET @start_time = GETDATE ()

		Print '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		Print '>> Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 ( cid , cntry )
		SELECT 
			REPLACE(cid , '-' , '') as cid , -- we replaced '-' to empty string 
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
				 WHEN TRIM(cntry) IN ( 'US' , 'USA' ) THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END cnrty -- we made data normalization 
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE ()
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ;

		Print '-----------------------------------------';
		SET @start_time = GETDATE ()

		Print '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2
		Print '>> Load Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 ( id , cat , subcat , maintenance )
		SELECT 
			id ,
			cat  ,
			subcat ,
			maintenance
		FROM bronze.erp_px_cat_g1v2 
		SET @end_time = GETDATE ()
		Print '>> Load Duration:' + CAST(DATEDIFF( Second , @start_time , @end_time ) as VARCHAR ) + ' Seconds' ;
		SET @batch_end_time = GETDATE () 
END TRY
BEGIN CATCH 
	Print'================================================' ;
	Print'-->> ERROR OCUURED DURING LOADING BRONZE LAYER       ' ;
	Print'Error Message: ' + Error_Message() ;
	Print'Error Number: ' + CAST( Error_Number() as VARCHAR ) ;
	Print'Error Line: ' + CAST( Error_Line() as VARCHAR ) ;
	Print'================================================' ;
END CATCH
	Print '-----------------------------------------'
	Print'================================================'
	Print'Loading Silver Layer is Completed'
	Print'--->> Total Load Duration:' + CAST(DATEDIFF( second , @batch_start_time , @batch_end_time ) as VARCHAR ) + ' Second'
	Print'================================================'

END 
