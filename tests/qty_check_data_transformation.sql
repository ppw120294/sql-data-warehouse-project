-- ===============================================================================
-- Checking - 'bronze.crm.cust.info'
-- ===============================================================================
-- Check for NULLs or Duplicate in Primary Key
-- Expectation: No Results

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check the standarization & consistency of values in low cardinality columns
-- Expectation: No Results
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


-- ===============================================================================
-- Checking - 'bronze.crm.prd.info'
-- ===============================================================================
-- Check for NULLs or Duplicate in Primary Key
-- Expectation: No Results
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM (prd_nm);


-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check the standarization & consistency of values in low cardinality columns
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HC-HL-U509-R', 'AC-HE-HL-U509');

-- ===============================================================================
-- Checking - 'bronze.crm_sales_details'
-- ===============================================================================
-- Check for unwanted spaces in string values
-- Expectation: No Results
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Check for sls_prd_key and sls_cust_id in order to connect with other tables
-- Expectation: No Results
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for Invalid Dates
-- Expectation: No Results
--========= sls_order_dt============
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;
-- Check Negative numbers or zero can't be cast to a date.

-- ============= sls_ship_dt =========================
SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101;

-- ============= sls_due_dt =========================
SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

-- Check for Invalid Dates Orders (sls_order_dt must always be earlier than sls_ship_dt and sls_due_dt)
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check for Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NUL, zero, or Negative.
-- >> If Sales is negative, zero, derive it using Quantity and Price
-- >> If Price is zero or null, calcualte it using Sales and Quantity
-- >> If Price is negative, convert it to a positive value
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ===============================================================================
-- Checking - 'bronze.erp_cust_az12'
-- ===============================================================================
-- Check for cid connect to crm_cust_info
-- Expectation: No Results
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%';

SELECT * FROM silver.crm_cust_info;

SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid 
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Check for Invalid Dates and boundaries
-- >> Check for birthdays old and in the future
SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-02' OR bdate > GETDATE();

-- Check for Data Standarization & Consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- ===============================================================================
-- Checking - 'bronze.erp_loc_a101'
-- ===============================================================================
-- Check for cid connect to crm_cust_info
-- Expectation: No Results
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info);

-- Data Standarization & Consistency
SELECT DISTINCT 
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


-- ===============================================================================
-- Checking - 'bronze.erp_px_cat_g1v2'
-- ===============================================================================
-- Check for id connect to crm_prd_info
-- Expectation: No Results
SELECT 
id
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info;

-- Check for Unwanted Spaces
SELECT 
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat!= TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standarization & Consistency
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;



-- ===============================================================================
-- Transforming - 'bronze.crm.cust.info'
-- ===============================================================================
SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- ROW_NUMBER() : Assigns a unique number to each row in a result set, based on a defined order.
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last =1;

-- ===============================================================================
-- Transforming - 'bronze.crm.prd.info'
-- ===============================================================================
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-', '_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'M' THEN 'Moutain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE )AS prd_end_dt
FROM bronze.crm_prd_info;

-- WHERE SUBSTRING(prd_key,7,LEN(prd_key)) IN(
-- SELECT sls_prd_key FROM bronze.crm_sales_details);

--WHERE REPLACE(SUBSTRING(prd_key,1,5),'-', '_') NOT IN
--(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

-- ===============================================================================
-- Transforming - 'bronze.crm.sales_details'
-- ===============================================================================
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

-- ===============================================================================
-- Transforming - 'bronze.erp_cust_az12'
-- ===============================================================================
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate> GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- ===============================================================================
-- Transforming - 'bronze.erp_loc_a101'
-- ===============================================================================
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- ===============================================================================
-- Transforming - 'bronze.erp_px_cat_g1v2;'
-- ===============================================================================
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
