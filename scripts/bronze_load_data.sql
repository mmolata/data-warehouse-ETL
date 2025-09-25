-- PROCEDURE: bronze.load_data()

-- DROP PROCEDURE IF EXISTS bronze.load_data();

CREATE OR REPLACE PROCEDURE bronze.load_data()
LANGUAGE plpgsql
AS $BODY$
BEGIN 

    RAISE NOTICE '==============================================================';
    RAISE NOTICE 'Loading CRM Data';
    RAISE NOTICE '==============================================================';

    -- CRM Customer Info
    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_crm\cust_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.crm_cust_info';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.crm_cust_info. Error: %', SQLERRM;
    END;

    -- CRM Product Info
    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_crm\prd_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.crm_prd_info';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.crm_prd_info. Error: %', SQLERRM;
    END;

    -- CRM Sales Details
    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_crm\sales_details.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.crm_sales_details';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.crm_sales_details. Error: %', SQLERRM;
    END;

    RAISE NOTICE '==============================================================';
    RAISE NOTICE 'Loading ERP Data';
    RAISE NOTICE '==============================================================';

    -- ERP Customer AZ12
    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_erp\CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.erp_cust_az12';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.erp_cust_az12. Error: %', SQLERRM;
    END;

    -- ERP Location A101
    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_erp\LOC_A101.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.erp_loc_a101';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.erp_loc_a101. Error: %', SQLERRM;
    END;

    -- ERP Product Category G1V2
    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM 'D:\projects\data-warehouse-ETL\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        RAISE NOTICE 'Successfully loaded bronze.erp_px_cat_g1v2';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load bronze.erp_px_cat_g1v2. Error: %', SQLERRM;
    END;

END;
$BODY$;

ALTER PROCEDURE bronze.load_data()
    OWNER TO postgres;
