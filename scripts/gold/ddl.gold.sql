/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    Script ini digunakan untuk membuat VIEW di layer GOLD.

    Gold layer adalah layer final di Data Warehouse
    yang berisi:
    - Dimension tables (dim_*)
    - Fact tables (fact_*)

    Struktur yang dihasilkan mengikuti konsep STAR SCHEMA
    sehingga siap digunakan untuk analytics & reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

-- Mengecek apakah view dim_customers sudah ada
-- Jika ada, maka dihapus terlebih dahulu
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

-- Membuat view dim_customers
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    -- Surrogate key (primary key dimensi)
    -- Dibuat secara sistematis, tidak berasal dari source system

    ci.cst_id AS customer_id,
    -- Natural key customer dari CRM

    ci.cst_key AS customer_number,
    -- Nomor customer dari sistem CRM

    ci.cst_firstname AS first_name,
    -- Nama depan customer

    ci.cst_lastname AS last_name,
    -- Nama belakang customer

    la.cntry AS country,
    -- Negara customer dari data lokasi ERP

    ci.cst_marital_status AS marital_status,
    -- Status pernikahan dari CRM

    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        -- CRM dianggap sebagai master data gender
        ELSE COALESCE(ca.gen, 'n/a')
        -- Jika CRM tidak punya data gender,
        -- gunakan data dari ERP sebagai fallback
    END AS gender,

    ca.bdate AS birthdate,
    -- Tanggal lahir customer dari ERP

    ci.cst_create_date AS create_date
    -- Tanggal customer dibuat di sistem CRM

FROM silver.crm_cust_info ci
-- Tabel customer dari CRM (silver layer)

LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
    -- Join ke ERP customer untuk data tambahan
    -- (birthdate, gender cadangan)

LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
    -- Join ke data lokasi customer
GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

-- Mengecek apakah view dim_products sudah ada
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

-- Membuat view dim_products
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    -- Surrogate key produk
    -- Digunakan sebagai primary key di dimensi produk

    pn.prd_id AS product_id,
    -- Natural key produk dari CRM

    pn.prd_key AS product_number,
    -- Kode produk dari CRM

    pn.prd_nm AS product_name,
    -- Nama produk

    pn.cat_id AS category_id,
    -- ID kategori produk

    pc.cat AS category,
    -- Nama kategori produk dari ERP

    pc.subcat AS subcategory,
    -- Subkategori produk

    pc.maintenance AS maintenance,
    -- Informasi maintenance produk

    pn.prd_cost AS cost,
    -- Biaya / cost produk

    pn.prd_line AS product_line,
    -- Tipe produk (Mountain, Road, Touring, dll)

    pn.prd_start_dt AS start_date
    -- Tanggal produk mulai aktif

FROM silver.crm_prd_info pn
-- Tabel produk dari CRM (silver layer)

LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
    -- Join ke master kategori produk dari ERP

WHERE pn.prd_end_dt IS NULL;
-- Filter hanya produk yang masih aktif
-- Produk yang sudah discontinued tidak ikut
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

-- Mengecek apakah view fact_sales sudah ada
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- Membuat view fact_sales
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    -- Nomor transaksi penjualan

    pr.product_key AS product_key,
    -- Foreign key ke dimensi produk

    cu.customer_key AS customer_key,
    -- Foreign key ke dimensi customer

    sd.sls_order_dt AS order_date,
    -- Tanggal order dibuat

    sd.sls_ship_dt AS shipping_date,
    -- Tanggal barang dikirim

    sd.sls_due_dt AS due_date,
    -- Tanggal jatuh tempo pembayaran

    sd.sls_sales AS sales_amount,
    -- Nilai total penjualan (measure utama)

    sd.sls_quantity AS quantity,
    -- Jumlah unit yang dijual

    sd.sls_price AS price
    -- Harga per unit produk

FROM silver.crm_sales_details sd
-- Tabel transaksi penjualan (grain: order + product)

LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
    -- Mapping product natural key → surrogate key

LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
    -- Mapping customer natural key → surrogate key
GO
