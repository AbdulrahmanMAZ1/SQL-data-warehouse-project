/* 
================================
Create Database and its schemas
================================
Script purpose :
	This script creates a new database called 'DataWareHouse' after checking if it already exists or not ,
	If it already exists SQL will force all active users to close and will drop the old database ,
	After that we create a new query to generate our Database 'DataWareHouse' and its schemas , 
	we divide the schemas for three layers ( Bronze , Silver , Gold ) . 

Warning : 
	Running this query will drop the entire 'DataWareHouse' database if it already exists ,
	Please make sure that you have a backup to the Database before running the script .
*/ 

USE master ; 
GO 

-- Drop and recreate the DataWareHouse Database 
IF EXISTS ( Select 1 from sys.databases Where name = 'Datawarehouse' )
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER With ROLLBACK IMMEDIATE ;
	DROP DATABASE DataWareHouse ;
END ; 
GO 

-- Create the 'Datawarehouse' database 
Create DATABASE DataWareHouse ;
GO 

USE DataWareHouse ;
GO 

-- Create Schemas 
Create Schema bronze ;

Go 

Create Schema silver ;

GO 

Create Schema gold ;
