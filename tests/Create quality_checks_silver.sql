/*
==========================================================================
Quality Checks
==========================================================================
This Script performs various quality checks for data consistency, accuracy,
and standardization across the silver schema. It includes checks for:
    1. Null or duplicate primary key.
    2. Unwanted space in string fields.
    3. Data standardization and consistency.
    4. Invalid date ranges and orders.
    5. Data consistency between related fields.

Usage Notes:
- Run these checks after data loading silver layer.
- Investigate and resolve any discrepancies found during the checks.
==========================================================================
*/
---------------------------------------------------
-- Checking 'silver.crm_cust_info'
---------------------------------------------------
-- check for null and duplicates in primary key
select cst_id,
COUNT(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- check for unwanted spaces
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

-- Data Standardization & Consistency
select distinct cst_gndr ,cst_marital_status
from silver.crm_cust_info

---------------------------------------------------
-- Checking 'silver.crm_prd_info'
---------------------------------------------------
-- check for null and duplicates in primary key
select prd_key,
COUNT(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null
  
-- check for unwanted spaces
  select prd_nm
from silver.crm_prd_info
where prd_nm  != TRIM(prd_nm);

--check for negative no and nulls
  select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & Consistency
  select distinct prd_line
from silver.crm_prd_info;

-- check invalid date orders
  select * from 
silver.crm_prd_info
where prd_start_dt > prd_end_dt;

---------------------------------------------------
-- Checking 'silver.crm_sales_details'
---------------------------------------------------
-- check for invalid details
  select nullif (sls_order_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or LEN(sls_order_dt) !=8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

--check for invalid date orders
select
  *
  from silver.crm_sales_details
  where sls_order_dt > sls_ship_dt
    or  sls_order_dt < sls_ship_dt;
  
-- check data consistency: between sales, quantity and price
  
select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales < = 0 or sls_quantity < = 0 or sls_price < = 0
order by sls_sales, sls_quantity, sls_price;

---------------------------------------------------
-- Checking 'silver.erp_cust_az12'
---------------------------------------------------
--identify out-of-range dates

select distinct
bdate 
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT
       gen
FROM   silver.erp_cust_az12;

---------------------------------------------------
-- Checking 'silver.erp_loc_a101'
---------------------------------------------------
-- Data Standardization & Consistency
SELECT DISTINCT
       cntry
FROM   silver.erp_loc_a101
order by cntry;

---------------------------------------------------
-- Checking 'silver.erp_px_cat_g1v2'
---------------------------------------------------
-- Check for Unwanted Spaces
SELECT *
FROM   silver.erp_px_cat_g1v2;
WHERE  cat!= trim(cat)
  or subcat != trim(subcat)
  or  maintenance != trim( maintenance);

---------------------------------------------------
-- Data Standardization & Consistency
SELECT DISTINCT
       maintenance
FROM  silver.erp_px_cat_g1v2;
