/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Usage: 
	This script creates views for the Gold Layer in the Data warehouse.
	The Gold Layer represents the final Dimension and Fact tables (Star Schema).

	Each view performs transformatins and combines data from the Silver Layer 
	to produce clean , enriched and business-ready dataset.

	Usage:
		- These views can be queired directly for analytics and reporting. 
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID ('gold.dim_customers' , 'V') IS NOT NULL
   DROP VIEW gold.dim_customers;	 	
GO
CREATE VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key ,
	ci.cst_id                           AS customer_id ,
	ci.cst_key                          AS customer_number ,
	ci.cst_firstname                    AS first_name ,
	ci.cst_lastname                     AS last_name ,
	la.cntry                            AS country ,
	CASE WHEN cst_gndr != 'n/a' THEN cst_gndr  -- CRM is the Master for gender 
		 ELSE COALESCE(ca.gen , 'n/a') 
	END                                 AS gender ,                   
	ci.cst_marital_status               AS marital_status ,
	ca.bdate                            AS birth_date ,
	ci.cst_create_date                  AS create_date 
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID ('gold.dim_products' , 'V') IS NOT NULL 
   DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt , prd_key ASC) AS product_key ,
	cr.prd_id       AS product_id ,
	cr.prd_key      AS product_number ,
	cr.prd_nm product_name ,
	cr.cat_id       AS category_id ,
	pc.cat          AS category ,
	pc.subcat       AS subcategory ,
	pc.maintenance ,
	cr.prd_cost     AS cost ,
	cr.prd_line     AS product_line ,
	cr.prd_start_dt AS start_date 
FROM silver.crm_prd_info cr -->> Master Tables 
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON cr.cat_id = pc.id 
WHERE prd_end_dt IS NULL; -- Filter out all historical data 
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID ('gold.fact_sales' , 'V') IS NOT NULL 
   DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT 
	sls_ord_num     AS order_name ,
	pr.product_key  AS product_key ,
	cr.customer_key AS customer_key ,
	sls_orders_dt   AS order_date ,
	sls_ship_dt     AS shipping_data ,
	sls_due_dt      AS due_date ,
	sls_sales       AS sales_amount ,
	sls_quantity    AS qunatity  ,
	sls_price       AS price 
FROM silver.crm_sales_details c
LEFT JOIN gold.dim_products pr
ON c.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cr
ON c.sls_cust_id = cr.customer_id;
GO
