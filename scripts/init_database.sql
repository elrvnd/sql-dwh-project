/* Creating Database & Schemas
Creates a new database named 'DataWareHouse' and drops existing database with the same name.
It also sets up three schemas: bronze, silver, and gold. 
This particular code is made with Microsoft SQL Server.

Proceed with caution! 
This script drops already existing 'DataWareHouse' database in your storage and permanently deletes it.
Make sure to have a backup before running this script.
*/

USE master;
GO

-- Drop & recreate 'DataWareHouse' database if already existing
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

-- Create database 'DataWareHouse'

CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

-- Create Schemas - in this case: three layers (bronze, silver, gold)
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO





