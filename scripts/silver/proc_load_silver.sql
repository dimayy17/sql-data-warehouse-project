/*
===============================================================================
Stored Procedure : silver.load_silver
===============================================================================
Tujuan:
- Melakukan proses ETL dari Bronze → Silver layer
- Membersihkan, menstandarisasi, dan menyiapkan data untuk analitik

Karakteristik:
- Full refresh (TRUNCATE + INSERT)
- Data cleansing (TRIM, REPLACE, validasi)
- Deduplication (ROW_NUMBER)
- Standardisasi value
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    -- =========================================================
    -- Variabel logging waktu
    -- =========================================================
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        -- =====================================================
        -- Tandai awal batch ETL
        -- =====================================================
        SET @batch_start_time = GETDATE();

        PRINT '========================================';
        PRINT 'START LOADING SILVER LAYER';
        PRINT '========================================';

        /* =====================================================
           LOAD: silver.crm_cust_info
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Insert silver.crm_cust_info';
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

            -- Hapus spasi & karakter aneh
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,

            -- Standardisasi status pernikahan
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,

            -- Standardisasi gender
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,

            cst_create_date
        FROM (
            SELECT *,
                   -- Deduplication:
                   -- Ambil data TERBARU per customer
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Done crm_cust_info : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =====================================================
           LOAD: silver.crm_prd_info
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Insert silver.crm_prd_info';
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

            -- Ambil category ID dari prd_key
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

            -- Ambil product key bersih
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

            prd_nm,

            -- NULL → 0 agar aman untuk agregasi
            ISNULL(prd_cost, 0) AS prd_cost,

            -- Normalisasi product line
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,

            CAST(prd_start_dt AS DATE) AS prd_start_dt,

            -- SCD Type 2 sederhana
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Done crm_prd_info : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =====================================================
           LOAD: silver.crm_sales_details
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Insert silver.crm_sales_details';
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

            -- Validasi & casting tanggal
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8
                 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,

            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8
                 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,

            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8
                 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,

            -- Validasi sales
            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,

            sls_quantity,

            -- Derive price jika invalid
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Done crm_sales_details : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =====================================================
           LOAD: silver.erp_cust_az12
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Insert silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            -- Hapus prefix NAS
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                 ELSE cid END,

            -- Validasi birthdate
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,

            -- Bersihkan karakter tersembunyi & standardisasi gender
            CASE
                WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(gen),
                     CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))
                     IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(gen),
                     CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))
                     IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Done erp_cust_az12 : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =====================================================
           LOAD: silver.erp_loc_a101
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Insert silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '') = 'DE'
                    THEN 'Germany'
                WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '') IN ('US','USA')
                    THEN 'United States'
                WHEN cntry IS NULL
                  OR REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '') = ''
                    THEN 'n/a'
                ELSE REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(10), '')
            END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Done erp_loc_a101 : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =====================================================
           LOAD: silver.erp_px_cat_g1v2
           ===================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Truncate silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Done erp_px_cat_g1v2 : ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        -- =====================================================
        -- END BATCH
        -- =====================================================
        SET @batch_end_time = GETDATE();
        PRINT '========================================';
        PRINT 'SILVER LAYER LOAD COMPLETED';
        PRINT 'TOTAL DURATION: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
              + ' sec';
        PRINT '========================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'ERROR DURING SILVER LOAD';
        PRINT ERROR_MESSAGE();
        PRINT '========================================';
    END CATCH
END;
