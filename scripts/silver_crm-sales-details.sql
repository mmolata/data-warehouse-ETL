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
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

