/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Tujuan Script:
- Membuat ulang (re-create) tabel-tabel pada schema SILVER
- Jika tabel sudah ada → akan di-drop dulu
- Digunakan setelah proses cleansing dari BRONZE ke SILVER
===============================================================================
*/

-- =========================================================
-- TABLE: silver.crm_cust_info
-- Menyimpan data master customer (hasil cleansing CRM)
-- =========================================================

-- Cek apakah tabel sudah ada
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;   -- Hapus tabel lama jika ada
GO

-- Buat ulang tabel customer
CREATE TABLE silver.crm_cust_info (
    cst_id             INT,            -- ID customer (numeric)
    cst_key            NVARCHAR(50),    -- Business key customer
    cst_firstname      NVARCHAR(50),    -- Nama depan customer
    cst_lastname       NVARCHAR(50),    -- Nama belakang customer
    cst_marital_status NVARCHAR(50),    -- Status pernikahan
    cst_gndr           NVARCHAR(50),    -- Gender
    cst_create_date    DATE,            -- Tanggal customer dibuat (source)
    dwh_create_date    DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()               -- Otomatis diisi saat data masuk
);
GO


-- =========================================================
-- TABLE: silver.crm_prd_info
-- Menyimpan data master produk
-- =========================================================

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,            -- ID produk (numeric)
    cat_id          NVARCHAR(50),    -- ID kategori produk
    prd_key         NVARCHAR(50),    -- Business key produk
    prd_nm          NVARCHAR(50),    -- Nama produk
    prd_cost        INT,             -- Cost produk
    prd_line        NVARCHAR(50),    -- Line / jenis produk
    prd_start_dt    DATE,            -- Tanggal mulai produk aktif
    prd_end_dt      DATE,            -- Tanggal akhir produk (jika ada)
    dwh_create_date DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()
);
GO


-- =========================================================
-- TABLE: silver.crm_sales_details
-- Menyimpan data transaksi penjualan
-- =========================================================

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),    -- Nomor order penjualan
    sls_prd_key     NVARCHAR(50),    -- Key produk yang dijual
    sls_cust_id     INT,             -- ID customer
    sls_order_dt    DATE,            -- Tanggal order
    sls_ship_dt     DATE,            -- Tanggal pengiriman
    sls_due_dt      DATE,            -- Tanggal jatuh tempo
    sls_sales       INT,             -- Total nilai penjualan
    sls_quantity    INT,             -- Jumlah unit terjual
    sls_price       INT,             -- Harga per unit
    dwh_create_date DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()
);
GO


-- =========================================================
-- TABLE: silver.erp_loc_a101
-- Menyimpan data lokasi / negara customer (ERP)
-- =========================================================

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),    -- Customer ID dari ERP
    cntry           NVARCHAR(50),    -- Negara customer
    dwh_create_date DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()
);
GO


-- =========================================================
-- TABLE: silver.erp_cust_az12
-- Menyimpan data demografi customer dari ERP
-- =========================================================

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),    -- Customer ID
    bdate           DATE,            -- Tanggal lahir
    gen             NVARCHAR(50),    -- Gender
    dwh_create_date DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()
);
GO


-- =========================================================
-- TABLE: silver.erp_px_cat_g1v2
-- Menyimpan kategori & subkategori produk (ERP)
-- =========================================================

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),    -- ID kategori
    cat             NVARCHAR(50),    -- Nama kategori
    subcat          NVARCHAR(50),    -- Sub-kategori
    maintenance     NVARCHAR(50),    -- Flag maintenance / status
    dwh_create_date DATETIME2        -- Timestamp insert ke DWH
        DEFAULT GETDATE()
);
GO
