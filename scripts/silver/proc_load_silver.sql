GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME,
            @end_time   DATETIME;

    /* ===============================
       Load silver.crm_cust_info
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    PRINT '>> Inserting Data Into: silver.crm_cust_info';

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
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(t.cst_gndr)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM DataWarehouse.bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';


    /* ===============================
       Load silver.crm_prd_info
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    PRINT '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - 1 AS DATE
        ) AS prd_end_dt
    FROM DataWarehouse.bronze.crm_prd_info;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';


    /* ===============================
       Load silver.crm_sales_details
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    PRINT '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
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
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
                 OR sls_sales <= 0
                 OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL
                 OR sls_price <= 0
                 OR sls_price <> sls_sales / sls_quantity
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM DataWarehouse.bronze.crm_sales_detail;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';


/* ===============================
       Load silver.erp_cust_az12
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.crm_sales_details;

    PRINT '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen)

    SELECT
    --Remove 'NAS' and SELECT after 4 fours label
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END cid,

    --fillter bdate before current day
    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END bdate,

    --Condition formating
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END gen
    FROM bronze.erp_cust_az12
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';

    /* ===============================
       Load silver.erp_loc_a101
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    PRINT '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO DataWarehouse.silver.erp_loc_a101(
    cid,
    cntry
    )
    SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN  UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
        WHEN  UPPER(TRIM(cntry)) IN ('USA', 'US','United States') THEN 'United States'
        WHEN  UPPER(TRIM(cntry)) IN ('AUSTRALIA') THEN 'Australia'
        WHEN  UPPER(TRIM(cntry)) IN ('UNITED KINFDOM') THEN 'United Kingdom'
        WHEN  UPPER(TRIM(cntry)) IN ('CANADA') THEN 'Canada'
        WHEN  UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
        ELSE 'n/a'
    END cntry
    FROM DataWarehouse.bronze.erp_loc_a101

    SELECT * FROM DataWarehouse.bronze.erp_loc_a101

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';

    /* ===============================
       Load silver.erp_px_cat_g1v2
       =============================== */
    SET @start_time = GETDATE();

    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    --NO need clean up(cuz source was perfect)
    INSERT INTO DataWarehouse.silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
    )
    SELECT
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '>> -------------';

    END;
GO

--EXEC silver.load_silver

