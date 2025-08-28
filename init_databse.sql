USE master;

----------------------------------------------------------------------------------------------------
/*
Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated.
*/

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

----------------------------------------------------------------------------------------------------
USE DataWarehouse;

CREATE SCHEMA bronze;
GO -- Separate batches when working with multiple SQL Statement
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

----------------------------------------------------------------------------------------------------
/*
============================================================
Create Bronze Tables
============================================================
Purpose:
  This query creates tables in the 'bronze' schema, dropping existing tables if they already exist.
*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL -- 'U' stands for user
  DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
  cst_id INT,
  cst_key NVARCHAR(50),
  cst_firstname NVARCHAR(50),
  cst_lastname NVARCHAR(50),
  cst_material_status NVARCHAR(50),
  cst_gender NVARCHAR(50),
  cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
  DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
  prd_id INT,
  prd_key NVARCHAR(50),
  prd_nm NVARCHAR(50),
  prd_cost INT,
  prd_line NVARCHAR(50),
  prd_start_dt DATETIME,
  prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
  DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
  sls_ord_num   NVARCHAR(50),
  sls_prd_key   NVARCHAR(50),
  sls_cust_id   INT,
  sls_order_dt  INT,
  sls_ship_dt   INT,
  sls_due_dt    INT,
  sls_sales     INT,
  sls_quantity  INT,
  sls_price     INT
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
  DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
  cid    NVARCHAR(50),
  cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
  DROP TABLE bronze.erp_cust_az12;
GO
  
CREATE TABLE bronze.erp_cust_az12 (
  cid    NVARCHAR(50),
  bdate  DATE,
  gen    NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_cat_g1v2', 'U') IS NOT NULL
  DROP TABLE bronze.erp_cat_g1v2;
GO

CREATE TABLE bronze.erp_cat_g1v2 (
  id          NVARCHAR(50),
  cat         NVARCHAR(50),
  subcat      NVARCHAR(50),
  maintenance NVARCHAR(50)
);
GO
----------------------------------------------------------------------------------------------------
/*
============================================================
Load data from sources into 'Bronze' Tables
============================================================
Purpose:

*/

TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'E:\DATA\sql-data-warehouse-project\datasets\cust_info.csv'
WITH (
  FIRSTROW = 2,
  FILEDTERMINATOR = ',',
  TABLOCK                     == lock the table during loading the entire table
);

TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'E:\DATA\sql-data-warehouse-project\datasets\prd_info.csv'
WITH (
  FIRSTROW = 2,
  FILEDTERMINATOR = ',',
  TABLOCK
);

TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM  'E:\DATA\sql-data-warehouse-project\datasets\sales_details.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  TABLOCK
);


