'DataWarehouse' setelah memeriksa apakah sudah ada.
Jika basis data tersebut ada, basis data tersebut akan dihapus dan dibuat ulang. Selain itu, skrip ini menyiapkan tiga skema
di dalam basis data: 'perunggu', 'perak', dan 'emas'.

PERINGATAN:
Menjalankan skrip ini akan menghapus seluruh basis data 'DataWarehouse' jika ada.
Semua data dalam basis data akan dihapus secara permanen. Lanjutkan dengan hati-hati
dan pastikan Anda memiliki cadangan yang tepat sebelum menjalankan skrip ini.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
