/* =========================================================
   STORED PROCEDURE : bronze.load_bronze
   PURPOSE          : Load data CSV ke Bronze Layer (Full Refresh)
   PLATFORM         : SQL Server 2022 Linux (Docker)
   ========================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze   -- Membuat / mengubah stored procedure di schema bronze
AS
BEGIN
    SET NOCOUNT ON;                            -- Menghilangkan pesan "(X rows affected)" agar log bersih

    DECLARE                                   -- Deklarasi variabel waktu
        @start_time DATETIME,                 -- Waktu mulai per tabel
        @end_time DATETIME,                   -- Waktu selesai per tabel
        @batch_start_time DATETIME,           -- Waktu mulai seluruh batch
        @batch_end_time DATETIME;             -- Waktu selesai seluruh batch

    BEGIN TRY                                 -- Awal TRY (error handling)
        SET @batch_start_time = GETDATE();    -- Simpan waktu mulai batch

        PRINT '==========================================';
        PRINT 'Loading Bronze Layer';          -- Header log
        PRINT '==========================================';

        /* ================= CRM TABLES ================= */

        SET @start_time = GETDATE();          -- Catat waktu mulai load tabel
        PRINT 'Truncate: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;  -- Full refresh (hapus semua data lama)

        PRINT 'Bulk Insert: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info      -- Load CSV ke tabel bronze
        FROM '/Data/cust_info.csv'             -- Path file CSV di container Linux
        WITH (
            FORMAT = 'CSV',                   -- Format file CSV
            FIRSTROW = 2,                     -- Skip header
            FIELDTERMINATOR = ',',            -- Pemisah kolom
            ROWTERMINATOR = '0x0a',            -- Line feed Linux
            TABLOCK                           -- Lock tabel untuk performa lebih cepat
        );

        SET @end_time = GETDATE();             -- Catat waktu selesai
        PRINT 'Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';                        -- Tampilkan durasi proses

        /* ------------------------------------------------ */

        SET @start_time = GETDATE();
        PRINT 'Truncate: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Bulk Insert: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/Data/prd_info.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';

        /* ------------------------------------------------ */

        SET @start_time = GETDATE();
        PRINT 'Truncate: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Bulk Insert: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/Data/sales_details.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';

        /* ================= ERP TABLES ================= */

        SET @start_time = GETDATE();
        PRINT 'Truncate: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Bulk Insert: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/Data/LOC_A101.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';

        /* ------------------------------------------------ */

        SET @start_time = GETDATE();
        PRINT 'Truncate: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Bulk Insert: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/Data/CUST_AZ12.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';

        /* ------------------------------------------------ */

        SET @start_time = GETDATE();
        PRINT 'Truncate: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT 'Bulk Insert: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/Data/PX_CAT_G1V2.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
              + ' sec';

        SET @batch_end_time = GETDATE();       -- Catat waktu akhir batch

        PRINT '==========================================';
        PRINT 'Bronze Layer Load Completed';
        PRINT 'Total Duration: '
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR)
              + ' sec';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH                               -- Jika terjadi error
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED';
        PRINT ERROR_MESSAGE();                -- Pesan error utama
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO
