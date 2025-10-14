/*
=========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze) 
=========================================================
Script Purpose : 
	This stored procedure loads the data from source system to the 'bronze' schema from external CSV file.
Action performed:
	- Truncate the 'bronze' tables before loading. 
	- Uses the `BULK INSERT` command to load the data from csv file to bronze tables. 

Usage Example: 
	EXEC bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME
	SET @batch_start_time = GETDATE()
	BEGIN TRY
		PRINT '======================================' ;
		PRINT '         Loadin Bronze Layer          ' ;
		PRINT '======================================' ;

		PRINT '---------------------------------------' ;
		Print '-->> Loading CRM Tables' ;
		PRINT '---------------------------------------' ;

		SET @start_time = GETDATE () 
		Print '>> Truncating Table: bronze.crm_cust_info' ;
		TRUNCATE Table bronze.crm_cust_info ;
		Print '>> Inserting Table: bronze.crm_cust_info' ;
		BULK INSERT bronze.crm_cust_info 
		FROM 'D:\My SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' , 
			TABLOCK 
		);
		SET @end_time = GETDATE () ;
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds' ;

		PRINT '---------------------------------------'

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: bronze.crm_prd_info' ;
		TRUNCATE Table bronze.crm_prd_info ;
		Print '>> Inserting Table: bronze.crm_prd_info' ;
		BULK INSERT bronze.crm_prd_info 
		From 'D:\My SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE ()
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds'
		
		PRINT '---------------------------------------'

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: bronze.crm_sales_details' ;
		TRUNCATE Table bronze.crm_sales_details ;
		Print '>> Inserting Table: bronze.crm_sales_details' ;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\My SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE ()
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds'


		PRINT '---------------------------------------' ;
		Print '-->> Loading ERP Tables' ;
		PRINT '---------------------------------------' ;

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: bronze.erp_cust_az12' ;
		TRUNCATE Table bronze.erp_cust_az12 ;
		Print '>> Inserting Table: bronze.erp_cust_az12' ;
		BULK INSERT bronze.erp_cust_az12 
		FROM 'D:\My SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @end_time = GETDATE ()
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds'
		
		PRINT '---------------------------------------'

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: bronze.erp_loc_a101' ;
		TRUNCATE Table bronze.erp_loc_a101 ;
		Print '>> Inserting Table: bronze.erp_loc_a101' ;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\My SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @end_time = GETDATE ()
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds'
		
		PRINT '---------------------------------------'

		SET @start_time = GETDATE ()
		Print '>> Truncating Table: bronze.erp_px_cat_g1v2' ;
		TRUNCATE Table bronze.erp_px_cat_g1v2 ;
		Print '>> Inserting Table: bronze.erp_px_cat_g1v2' ;
		BULK INSERT bronze.erp_px_cat_g1v2 
		FROM 'D:\My SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @end_time = GETDATE ()
		PRINT '>> Load Durantion:' + CAST(DATEDIFF(Second , @start_time , @end_time ) as NVARCHAR) + ' Seconds'
		
		PRINT '---------------------------------------'

	END TRY 
	BEGIN CATCH 
		Print'=========================================='
		PRINT 'ERROR OCUURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE () ;
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER () as NVARCHAR ) ;
		PRINT 'Error Line: ' + CAST ( ERROR_LINE () AS NVARCHAR ) 
		PRINT '==========================================';
	END CATCH 
	SET @batch_end_time = GETDATE() ;
		Print'=========================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '--->> Total Load Duration:' + CAST(DATEDIFF(Second , @batch_start_time , @batch_end_time) AS VARCHAR ) + ' seconds'
		Print'=========================================='
END

