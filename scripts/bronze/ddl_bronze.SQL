/*
===================================================================================
DDL Script: Create Bronze Tables
===================================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables
if they already exist.
Run this script to re_define the DDL structure of 'bronze' Tables 
===================================================================================
*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') is not null
	DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
	cst_id			int,
	cst_key			 nvarchar(50),
	cst_firstname	 nvarchar(50),
	cst_lastname	 nvarchar(50),	
	cst_marital_status		nvarchar(50),
	cst_gndr           nvarchar(50),
	cst_create_date      date
	);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id       int,
	prd_key      nvarchar(50),
	prd_nm       nvarchar(50),
	prd_cost     int,
	prd_line     nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt   datetime
); 
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num    nvarchar(50),
	sls_prd_key    nvarchar(50),
	sls_cust_id    int,
	sls_order_dt   int,
	sls_ship_dt    int,
	sls_due_dt     int,
	sls_sales      int,
	sls_quantity   int,
	sls_price      int
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') is not null
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid    nvarchar(50),
	cntry  nvarchar(50)
);
GO

IF OBJECT_ID ('bronze.erp_cust_az01', 'U') is not null
	DROP TABLE bronze.erp_cust_az01;
 CREATE TABLE bronze.erp_cust_az01 (
	 cid   nvarchar(50),
	 bdate date,
	 gen   nvarchar(50)
 );
GO

 IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') is not null
	DROP TABLE bronze.erp_px_cat_g1v2;
 CREATE TABLE bronze.erp_px_cat_g1v2 (
	 id            nvarchar(50),
	 cat           nvarchar(50),
	 subcat        nvarchar(50),
	 maintenance   nvarchar(50)
 );
