-- Insert cleaned crm_cust_info
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_recent
    FROM bronze.crm_cust_info
) t
WHERE flag_recent = 1;


--insert cleaned crm_prd_info
INSERT INTO silver.crm_prd_info 
(
prd_id,
prd_cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)


	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'Unknown'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS new_prd_end_dt
	FROM bronze.crm_prd_info

select * from silver.crm_prd_info;

--insert cleaned crm_sales_details

INSERT INTO silver.crm_sales_details
(
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


SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 
	OR LENGTH(sls_order_dt::text) != 8
	OR sls_order_dt > 20501230
	OR sls_order_dt < 19500101
	THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END as sls_order_dt,
CASE WHEN sls_ship_dt = 0 
	OR LENGTH(sls_ship_dt::text) != 8
	OR sls_ship_dt > 20501230
	OR sls_ship_dt < 19500101
	THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END as sls_ship_dt,
CASE WHEN sls_due_dt = 0 
	OR LENGTH(sls_due_dt::text) != 8
	OR sls_due_dt > 20501230
	OR sls_due_dt < 19500101
	THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END as sls_due_dt,
CASE WHEN sls_sales <= 0 
		OR sls_sales IS NULL
		OR sls_sales != (sls_quantity*sls_price)
		THEN  (sls_quantity*ABS(sls_price)) 
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price = 0 
			OR sls_price IS NULL
			THEN (sls_sales/NULLIF(sls_quantity,0))
		WHEN sls_price < 0 
			THEN ABS(sls_price)
		ELSE sls_price
	END as sls_price
FROM bronze.crm_sales_details;

--insert cleaned erp_cust_az12

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)

select 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid FROM 4)
	ELSE cid
END as cid,
CASE WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END as bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	ELSE NULL
END as gen
from bronze.erp_cust_az12;

--insert cleaned erp_loc_a101

INSERT INTO silver.erp_loc_a101
(
cid,
cntry
)

select 
CASE WHEN cid LIKE 'AW-%' THEN REPLACE(cid,'-','')
	ELSE NULL
END as cid,
CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES') THEN 'United States'
	WHEN TRIM(cntry) = '' THEN NULL
	WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
 	ELSE cntry
END as cntry
from bronze.erp_loc_a101;

--insert cleaned erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2
(
id,
cat,
subcat,
maintenance
)

select * from bronze.erp_px_cat_g1v2










