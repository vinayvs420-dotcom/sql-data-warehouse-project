/*
==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to populate Silver schema tables from the Bronze schema.
Actions Performed:
    - Truncate Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC Silver.Load_Silver;
==============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT '===================================================';
		PRINT 'Loading silver Layer';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info; 

		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,	
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	select 		
		cst_id,
		cst_key,
		trim (cst_firstname) as cst_firstname,
		trim (cst_lastname) as cst_lastname,
	case
			when upper(trim (cst_marital_status)) = 'S' then 'Single'
			when upper(trim (cst_marital_status)) = 'M' then 'Married'
		else 'n/a'
		end
		cst_marital_status, 
	case
			when upper(trim (cst_gndr)) = 'F' then 'Female'
			when upper(trim (cst_gndr)) = 'M' then 'Male'
		else 'n/a'
		end
		cst_gndr,             
		cst_create_date from 
		(
	select
		*,
	ROW_NUMBER () over (partition by cst_id order by cst_create_date) as flag
	from bronze.crm_cust_info
	where cst_id is not null
	) t
	where flag = 1; 
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
	PRINT '>> --------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info ;

	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
	select
		prd_id,
	replace(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id, 
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, 
		prd_nm,
	isnull (prd_cost, 0) as prd_cost,
	case when UPPER(trim(prd_line)) = 'M' then 'Mountain'
	  when UPPER(trim(prd_line)) = 'R' then 'Road'
	  when UPPER(trim(prd_line)) = 'S' then 'Other Sales'
	  when UPPER(trim(prd_line)) = 'T' then 'Touring'
	else 'n/a'
	end as prd_line, 
	cast (prd_start_dt as date) as prd_start_date,
	CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS date) AS prd_end_dt
	from bronze.crm_prd_info;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
		PRINT '>> --------------';
	
	SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
	case when sls_order_dt = 0 OR LEN(sls_order_dt) != 8 then null
		else CAST(cast(sls_order_dt as varchar) AS date)
	end sls_order_dt,
	case when sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 then null
		else CAST(cast(sls_ship_dt as varchar) AS date)
	end sls_ship_dt,
	case when sls_due_dt = 0 OR LEN(sls_due_dt) != 8 then null
		else CAST(cast(sls_due_dt as varchar) AS date)
	end sls_due_dt,
	case when sls_sales IS null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * ABS(sls_price)
		else sls_sales
	end as sls_sales, 
	sls_quantity,
	case when sls_quantity IS null or sls_price <= 0
	then sls_sales / nullif (sls_quantity,0)
	else sls_price 
	end as sls_price
	from bronze.crm_sales_details;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
		PRINT '>> --------------';


	SET @start_time = GETDATE();
		
		PRINT '---------------------------------------------------';
		PRINT 'Loading	ERP Tables';
		PRINT '---------------------------------------------------';

		PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12 ;

		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        
		insert into silver.erp_cust_az12 (cid,bdate,gen)
		select 
		case when cid like 'NAS%' then SUBSTRING (cid,4,len(cid)) 
			else cid
		end as cid,
		case when bdate > GETDATE() then null
			else bdate 
		end bdate, 
		case when UPPER(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
	 		when UPPER(trim(gen)) in ('M', 'MALE') THEN 'Male'
	 		Else 'n/a'
	 	end as gen 
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
		PRINT '>> --------------';

               SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101
		(cid,cntry)
	select
		replace(cid, '-', '') cid,
		case when TRIM(cntry) = 'DE' then 'Germany'
	 		when TRIM(cntry) in ('US','USA') then 'United States'
			 when TRIM(cntry) = '' or cntry is null then 'n/a'
		else TRIM(cntry)
		end as cntry 
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
		PRINT '>> --------------';

                SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
	select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) as NVARCHAR) + 'SECOND';
		PRINT '>> --------------';

		SET @batch_end_time = GETDATE();
		PRINT '============================================='
		PRINT 'Loading silver layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) as NVARCHAR) + 'SECOND'
		PRINT '============================================='

	END TRY
	BEGIN CATCH
	    PRINT '============================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================='
	END CATCH
END
