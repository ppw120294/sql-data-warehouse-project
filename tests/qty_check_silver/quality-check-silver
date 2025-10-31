/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy
	and standardization across the 'silver' layer. It includes checks for:
	- Null or duplicate primary key.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fileds.

Usage Notes:
	- Run these checks after data Loading Silver Layer.
	- Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ===============================================================================
-- Checking - 'silver.crm.cust.info'
-- ===============================================================================
-- Check for NULLs or Duplicate in Primary Key
-- Expectation: No Results

USE DataWarehouse
GO

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check the standarization & consistency of values in low cardinality columns
-- Expectation: No Results
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ===============================================================================
-- Checking - 'bronze.crm.prd.info'
-- ===============================================================================
-- Check for NULLs or Duplicate in Primary Key
-- Expectation: No Results
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM (prd_nm);


-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check the standarization & consistency of values in low cardinality columns
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ===============================================================================
-- Checking - 'silver.crm_sales_details'
-- ===============================================================================
-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT
*
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check for sls_prd_key and sls_cust_id in order to connect with other tables
-- Expectation: No Results
SELECT
*
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT
*
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for Invalid Dates
-- Expectation: No Results
SELECT
sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM silver.crm_sales_details;

-- Check for Invalid Dates Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check for Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NUL, zero, or Negative.
-- >> If Sales is negative, zero, derive it using Quantity and Price
-- >> If Price is zero or null, calcualte it using Sales and Quantity
-- >> If Price is negative, convert it to a positive value
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;


-- ===============================================================================
-- Checking - 'silver.erp_cust_az12'
-- ===============================================================================
-- Check for cid connect to crm_cust_info
-- Expectation: No Results
SELECT
cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE cid LIKE '%AW00011000%';

SELECT
cid
FROM silver.erp_cust_az12;

-- Check for Out-Of-Range Dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Check for Data Standarization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12;

-- ===============================================================================
-- Checking - 'silver.erp_loc_a101'
-- ===============================================================================
-- Check for Data Standarization & Consistency
-- Expectation: No Results
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT * FROM silver.erp_loc_a101;


-- ===============================================================================
-- Checking - 'silver.erp_px_cat_g1v2'
-- ===============================================================================
-- Check for id connect to crm_prd_info
-- Expectation: No Results
SELECT 
id
FROM silver.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info;

-- Check for Unwanted Spaces
SELECT 
*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat!= TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standarization & Consistency
SELECT DISTINCT
cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
subcat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2;
