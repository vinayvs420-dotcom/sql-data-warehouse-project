/*
============================================================
    Quality Checks
============================================================ 
    Script Purpose:
    This script performs quality checks to validate the integrity, consistency, and accuracy of the data loaded into the Silver layer.

    These checks ensure:
    - Referential integrity between the fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

    Usage Notes:
   - Run these checks after data loading Silver Layer.
   - Investigate and resolve any discrepancies found during the checks.
============================================================
*/

 ----------------------------------------
    Checking 'Gold.dim_customers'
---------------------------------------- 
select 
	customer_key,
COUNT(*) as duplicate_count
from gold.dim_customers
group by customer_key
having COUNT(*) > 1;
----------------------------------------
    Checking 'Gold.dim_products'
---------------------------------------- 
select
	product_key,
COUNT(*) as duplicate_count
from gold.dim_products
group by product_key
having COUNT(*) > 1;
----------------------------------------
    Checking 'Gold.fact_sales'
---------------------------------------- 
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null or c.customer_key is null;
