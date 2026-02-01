-- Sprint 1
-- Task 1.3.1 Create project database and schemas in Snowflake
CREATE DATABASE IF NOT EXISTS CONTOSO_DB;

CREATE SCHEMA IF NOT EXISTS CONTOSO_DB.RAW_CONTOSO;

CREATE SCHEMA IF NOT EXISTS CONTOSO_DB.DW_CONTOSO;

USE DATABASE CONTOSO_DB;
SHOW SCHEMAS;

-- Task 1.3.2 Setting up stages and file formats for Contoso files
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

CREATE OR REPLACE FILE FORMAT contoso_file_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','           
  SKIP_HEADER = 1                 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
  NULL_IF = ('NULL', 'null', '')  
  EMPTY_FIELD_AS_NULL = TRUE;     

  CREATE STAGE contoso_stage
  FILE_FORMAT = contoso_file_format;

SHOW STAGES LIKE 'contoso_stage';
LIST @contoso_stage;

--Task 1.3.3 Load table A into RAW_CONTOSO
CREATE OR REPLACE TABLE RAW_DIMPRODUCT (
    ProductKey INT,
    ProductName STRING,
    ProductDescription STRING,
    ProductSubcategoryKey INT,
    Manufacturer STRING,
    BrandName STRING,
    ClassID STRING,
    ClassName STRING,
    StyleID STRING,
    StyleName STRING,
    ColorID STRING,
    ColorName STRING,
    Weight FLOAT,
    WeightUnitMeasureID STRING,
    UnitOfMeasureID STRING,    
    UnitOfMeasureName STRING,  
    StockTypeID STRING,        
    StockTypeName STRING,      
    UnitCost FLOAT,            
    UnitPrice FLOAT,           
    AvailableForSaleDate TIMESTAMP, 
    Status STRING              
);

COPY INTO RAW_DIMPRODUCT
FROM @CONTOSO_STAGE/DimProduct.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'CONTINUE'; 

SELECT COUNT(*) FROM RAW_DIMPRODUCT;

--Task 1.3.4 Load table B into RAW_CONTOSO
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

CREATE OR REPLACE TABLE RAW_DIMCUSTOMER (
    CustomerKey INT,
    GeographyKey INT,
    FirstName STRING,
    LastName STRING,
    BirthDate DATE,             
    MaritalStatus STRING,       
    Gender STRING,              
    YearlyIncome FLOAT,         
    TotalChildren INT,
    NumberChildrenAtHome INT,
    Education STRING,
    Occupation STRING,
    HouseOwnerFlag INT,         
    NumberCarsOwned INT         
);

--Load Data with Date Format Fix
COPY INTO RAW_DIMCUSTOMER
FROM @CONTOSO_STAGE/DimCustomer.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '')
    DATE_FORMAT = 'DD/MM/YYYY'   
)
ON_ERROR = 'CONTINUE';

--Verify it worked
SELECT COUNT(*) FROM RAW_DIMCUSTOMER;

--Task 1.4.1 Profile Contoso table A 

-- 1. Set Context
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- 2. Basic Volume & Primary Key Check
SELECT 
    COUNT(*) AS Total_Rows,
    COUNT(DISTINCT ProductKey) AS Unique_Keys,
    (COUNT(*) - COUNT(DISTINCT ProductKey)) AS Duplicate_Keys
FROM RAW_DIMPRODUCT;

-- 3. Date Range Profiling
SELECT 
    MIN(AvailableForSaleDate) AS First_Sale_Date,
    MAX(AvailableForSaleDate) AS Last_Sale_Date
FROM RAW_DIMPRODUCT;

-- 4. Null Checks on Critical Columns
SELECT 
    COUNT(*) AS Null_Names 
FROM RAW_DIMPRODUCT 
WHERE ProductName IS NULL;

-- 5. Data Quality Check (Negative Prices)
SELECT 
    COUNT(*) AS Invalid_Prices 
FROM RAW_DIMPRODUCT 
WHERE UnitPrice < 0 OR UnitCost < 0;

-- 6. Inspect Sample Data
SELECT * FROM RAW_DIMPRODUCT LIMIT 10;

-- Task 1.4.2 Profile Contoso DimCustomer
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;


SELECT 
    COUNT(*) AS total_rows, 
    
    COUNT(DISTINCT CustomerKey) AS unique_customer_keys,
    
    MIN(BirthDate) AS oldest_customer_birth, 
    MAX(BirthDate) AS youngest_customer_birth,
    
    COUNT(*) - COUNT(CustomerKey) AS null_CustomerKey,
    COUNT(*) - COUNT(GeographyKey) AS null_GeographyKey,
    COUNT(*) - COUNT(YearlyIncome) AS null_YearlyIncome,
    
    MIN(YearlyIncome) AS min_income,
    MAX(YearlyIncome) AS max_income
FROM CONTOSO_DB.RAW_CONTOSO.RAW_DIMCUSTOMER;





-- Sprint 2
-- Task 2.1.1
USE SCHEMA RAW_CONTOSO;

-- 1. Ensure Table exists (Matching your columns)
CREATE OR REPLACE TABLE RAW_FACTSALES (
    SalesKey INT,
    DateKey DATETIME,
    channelKey INT,       
    StoreKey INT,
    ProductKey INT,
    PromotionKey INT,
    CurrencyKey INT,
    UnitCost DECIMAL(10,2),
    UnitPrice DECIMAL(10,2),
    SalesQuantity INT,
    ReturnQuantity INT,
    ReturnAmount DECIMAL(10,2),
    DiscountQuantity INT,
    DiscountAmount DECIMAL(10,2),
    TotalCost DECIMAL(10,2),
    SalesAmount DECIMAL(10,2)
);

USE SCHEMA RAW_CONTOSO;

COPY INTO RAW_FACTSALES
FROM @CONTOSO_STAGE
PATTERN = '.*FactSales_.*.csv'   
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,             
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    NULL_IF = ('NULL', 'null', ''),
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';       

-- Task 2.1.2 
USE SCHEMA RAW_CONTOSO;

CREATE OR REPLACE TABLE RAW_FACT_ONLINESALES (
    -- The Keys
    OnlineSalesKey INT,
    DateKey DATETIME,
    StoreKey INT,
    ProductKey INT,
    PromotionKey INT,
    CurrencyKey INT,
    CustomerKey INT,
    
    -- The Order Details
    SalesOrderNumber VARCHAR(50),
    SalesOrderLineNumber INT,
    
    -- The Metrics (Sales, Returns, Discounts, Costs)
    SalesQuantity INT,
    SalesAmount DECIMAL(10,2),
    ReturnQuantity INT,
    ReturnAmount DECIMAL(10,2),
    DiscountQuantity INT,
    DiscountAmount DECIMAL(10,2),
    TotalCost DECIMAL(10,2),
    UnitCost DECIMAL(10,2),
    UnitPrice DECIMAL(10,2)
);

COPY INTO RAW_FACT_ONLINESALES
FROM @CONTOSO_STAGE
PATTERN = '.*FactOnlineSales_.*.csv'
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,               
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    NULL_IF = ('NULL', 'null', ''),
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';    

-- Task 2.1.3
SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACTSALES;
SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACT_ONLINESALES;

--  Task 2.2.1
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

SELECT
  $1, $2, $3, $4
FROM @CONTOSO_STAGE/DimStore.csv
LIMIT 10;

CREATE OR REPLACE TABLE RAW_DIMSTORE (
  StoreKey         INTEGER,
  GeographyKey      INTEGER,
  StoreManager      INTEGER,
  StoreType         VARCHAR,
  StoreName         VARCHAR,
  Status            VARCHAR,
  OpenDate          DATE,
  CloseDate         DATE,
  EntityKey         INTEGER,
  StorePhone        VARCHAR,
  StoreFax          VARCHAR,
  CloseReason       VARCHAR,
  EmployeeCount     INTEGER,
  SellingAreaSize   INTEGER,
  LastRemodelDate   TIMESTAMP_NTZ,
  EmployeeKey       INTEGER
);

COPY INTO RAW_DIMSTORE
FROM @CONTOSO_STAGE/DimStore.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

---
SELECT COUNT(*) FROM RAW_DIMSTORE;

---

SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT StoreKey) AS distinct_keys
FROM RAW_DIMSTORE;

---- 
SELECT * FROM RAW_DIMSTORE LIMIT 10;

USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

SELECT
  $1, $2, $3, $4, $5, $6
FROM @CONTOSO_STAGE/DimGeography.csv
LIMIT 10;

--- Create Table 

CREATE OR REPLACE TABLE RAW_DIMGEOGRAPHY (
  GeographyKey        INTEGER,
  GeographyType       VARCHAR,
  ContinentName       VARCHAR,
  CityName            VARCHAR,
  StateProvinceName   VARCHAR,
  RegionCountryName   VARCHAR
);

--- COPY INTO RAW_DIMGEOGRAPHY

COPY INTO RAW_DIMGEOGRAPHY
FROM @CONTOSO_STAGE/DimGeography.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

--- Validation 

SELECT COUNT(*) FROM RAW_DIMGEOGRAPHY;

--- Primary Key Sanity Check

SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT GeographyKey) AS distinct_keys
FROM RAW_DIMGEOGRAPHY;

--- Sample data check

SELECT * FROM RAW_DIMGEOGRAPHY LIMIT 20;

-- Task 2.2.2
-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- (Optional) Confirm the file exists in the stage
LIST @CONTOSO_STAGE;

-- (Optional) Preview staged data (header should already be skipped by file format)
SELECT
  $1 AS ProductCategoryKey,
  $2 AS ProductCategoryName
FROM @CONTOSO_STAGE/DimProductCategory.csv
LIMIT 10;

-- Create RAW table (matches your CSV headers)
CREATE OR REPLACE TABLE RAW_DIMPRODUCTCATEGORY (
  ProductCategoryKey   INTEGER,
  ProductCategoryName  VARCHAR
);

-- Load
COPY INTO RAW_DIMPRODUCTCATEGORY
FROM @CONTOSO_STAGE/DimProductCategory.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validate: row count
SELECT COUNT(*) AS total_rows
FROM RAW_DIMPRODUCTCATEGORY;

-- Validate: key uniqueness
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT ProductCategoryKey) AS distinct_keys
FROM RAW_DIMPRODUCTCATEGORY;

-- Validate: quick sample
SELECT *
FROM RAW_DIMPRODUCTCATEGORY
ORDER BY ProductCategoryKey
LIMIT 20;

-- Validate: missing/blank checks (should be 0 ideally)
SELECT
  SUM(CASE WHEN ProductCategoryKey IS NULL THEN 1 ELSE 0 END) AS null_key_rows,
  SUM(CASE WHEN ProductCategoryName IS NULL OR TRIM(ProductCategoryName) = '' THEN 1 ELSE 0 END) AS blank_name_rows
FROM RAW_DIMPRODUCTCATEGORY;

-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- (Optional) Confirm the file exists in the stage
LIST @CONTOSO_STAGE;

-- (Optional) Preview staged data
-- Note: Your screenshot shows columns:
-- ProductSubcategoryKey, ProductSubcategoryName, ProductCategoryKey
SELECT
  $1 AS ProductSubcategoryKey,
  $2 AS ProductSubcategoryName,
  $3 AS ProductCategoryKey
FROM @CONTOSO_STAGE/DimProductSubcategory.csv
LIMIT 10;

-- Create RAW table (matches your CSV headers)
CREATE OR REPLACE TABLE RAW_DIMPRODUCTSUBCATEGORY (
  ProductSubcategoryKey    INTEGER,
  ProductSubcategoryName   VARCHAR,
  ProductCategoryKey       INTEGER
);

-- Load
COPY INTO RAW_DIMPRODUCTSUBCATEGORY
FROM @CONTOSO_STAGE/DimProductSubcategory.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validate: row count
SELECT COUNT(*) AS total_rows
FROM RAW_DIMPRODUCTSUBCATEGORY;

-- Validate: key uniqueness for subcategory key
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT ProductSubcategoryKey) AS distinct_keys
FROM RAW_DIMPRODUCTSUBCATEGORY;

-- Validate: quick sample
SELECT *
FROM RAW_DIMPRODUCTSUBCATEGORY
ORDER BY ProductSubcategoryKey
LIMIT 30;

-- Validate: missing/blank checks
SELECT
  SUM(CASE WHEN ProductSubcategoryKey IS NULL THEN 1 ELSE 0 END) AS null_subcat_key_rows,
  SUM(CASE WHEN ProductCategoryKey IS NULL THEN 1 ELSE 0 END) AS null_cat_key_rows,
  SUM(CASE WHEN ProductSubcategoryName IS NULL OR TRIM(ProductSubcategoryName) = '' THEN 1 ELSE 0 END) AS blank_name_rows
FROM RAW_DIMPRODUCTSUBCATEGORY;

-- Relationship sanity check: orphan subcategories (CategoryKey in subcat not found in category)
SELECT
  s.ProductCategoryKey,
  COUNT(*) AS orphan_subcategory_rows
FROM RAW_DIMPRODUCTSUBCATEGORY s
LEFT JOIN RAW_DIMPRODUCTCATEGORY c
  ON s.ProductCategoryKey = c.ProductCategoryKey
WHERE c.ProductCategoryKey IS NULL
GROUP BY s.ProductCategoryKey
ORDER BY orphan_subcategory_rows DESC;

-- Relationship sanity check: count subcategories per category (useful profiling evidence)
SELECT
  c.ProductCategoryKey,
  c.ProductCategoryName,
  COUNT(s.ProductSubcategoryKey) AS subcategory_count
FROM RAW_DIMPRODUCTCATEGORY c
LEFT JOIN RAW_DIMPRODUCTSUBCATEGORY s
  ON c.ProductCategoryKey = s.ProductCategoryKey
GROUP BY c.ProductCategoryKey, c.ProductCategoryName
ORDER BY c.ProductCategoryKey;

-- Task 2.2.3
-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- Optional: confirm file exists
LIST @CONTOSO_STAGE;

-- Preview staged data
SELECT
  $1 AS ChannelKey,
  $2 AS ChannelName,
  $3 AS ChannelDescription
FROM @CONTOSO_STAGE/DimChannel.csv
LIMIT 10;

-- Create RAW table
CREATE OR REPLACE TABLE RAW_DIMCHANNEL (
  ChannelKey          INTEGER,
  ChannelName         VARCHAR,
  ChannelDescription  VARCHAR
);

-- Load data
COPY INTO RAW_DIMCHANNEL
FROM @CONTOSO_STAGE/DimChannel.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validation
SELECT COUNT(*) AS total_rows
FROM RAW_DIMCHANNEL;

SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT ChannelKey) AS distinct_keys
FROM RAW_DIMCHANNEL;

SELECT *
FROM RAW_DIMCHANNEL
ORDER BY ChannelKey
LIMIT 20;

-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- Optional: confirm file exists
LIST @CONTOSO_STAGE;

-- Preview staged data
SELECT
  $1 AS CurrencyKey,
  $2 AS CurrencyName,
  $3 AS CurrencyDescription
FROM @CONTOSO_STAGE/DimCurrency.csv
LIMIT 10;

-- Create RAW table
CREATE OR REPLACE TABLE RAW_DIMCURRENCY (
  CurrencyKey          INTEGER,
  CurrencyName         VARCHAR,
  CurrencyDescription  VARCHAR
);

-- Load data
COPY INTO RAW_DIMCURRENCY
FROM @CONTOSO_STAGE/DimCurrency.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validation
SELECT COUNT(*) AS total_rows
FROM RAW_DIMCURRENCY;

SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CurrencyKey) AS distinct_keys
FROM RAW_DIMCURRENCY;

SELECT *
FROM RAW_DIMCURRENCY
ORDER BY CurrencyKey
LIMIT 20;

-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- Optional: confirm file exists
LIST @CONTOSO_STAGE;

-- Preview staged data
SELECT
  $1  AS EmployeeKey,
  $2  AS ParentEmployeeKey,
  $3  AS FirstName,
  $4  AS LastName,
  $5  AS Title,
  $6  AS HireDate,
  $7  AS BirthDate,
  $8  AS EmailAddress,
  $9  AS Phone,
  $10 AS EmergencyContactName,
  $11 AS EmergencyContactPhone,
  $12 AS Gender,
  $13 AS PayFrequency,
  $14 AS BaseRate,
  $15 AS VacationHours,
  $16 AS DepartmentName,
  $17 AS StartDate,
  $18 AS Status,
  $19 AS SalaryStatus,
  $20 AS IsSalesPerson,
  $21 AS IsMarried
FROM @CONTOSO_STAGE/DimEmployee.csv
LIMIT 10;

-- Create RAW table
CREATE OR REPLACE TABLE RAW_DIMEMPLOYEE (
  EmployeeKey              INTEGER,
  ParentEmployeeKey        INTEGER,
  FirstName                VARCHAR,
  LastName                 VARCHAR,
  Title                    VARCHAR,
  HireDate                 DATE,
  BirthDate                DATE,
  EmailAddress             VARCHAR,
  Phone                    VARCHAR,
  EmergencyContactName     VARCHAR,
  EmergencyContactPhone    VARCHAR,
  Gender                   VARCHAR,
  PayFrequency             INTEGER,
  BaseRate                 FLOAT,
  VacationHours            INTEGER,
  DepartmentName           VARCHAR,
  StartDate                DATE,
  Status                   VARCHAR,
  SalaryStatus             VARCHAR,
  IsSalesPerson            VARCHAR,
  IsMarried                VARCHAR
);

-- Load data
COPY INTO RAW_DIMEMPLOYEE
FROM @CONTOSO_STAGE/DimEmployee.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validation
SELECT COUNT(*) AS total_rows
FROM RAW_DIMEMPLOYEE;

SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT EmployeeKey) AS distinct_keys
FROM RAW_DIMEMPLOYEE;

-- Sample check
SELECT *
FROM RAW_DIMEMPLOYEE
ORDER BY EmployeeKey
LIMIT 20;

-- Context
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- Optional: confirm file exists
LIST @CONTOSO_STAGE;

-- Preview staged data
SELECT
  $1 AS PromotionKey,
  $2 AS PromotionName,
  $3 AS DiscountPercent,
  $4 AS PromotionType,
  $5 AS PromotionCategory
FROM @CONTOSO_STAGE/DimPromotion.csv
LIMIT 10;

-- Create RAW table (matches CSV)
CREATE OR REPLACE TABLE RAW_DIMPROMOTION (
  PromotionKey        INTEGER,
  PromotionName       VARCHAR,
  DiscountPercent     FLOAT,
  PromotionType       VARCHAR,
  PromotionCategory   VARCHAR
);

-- Load data
COPY INTO RAW_DIMPROMOTION
FROM @CONTOSO_STAGE/DimPromotion.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Validation: row count
SELECT COUNT(*) AS total_rows
FROM RAW_DIMPROMOTION;

-- Validation: key uniqueness
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT PromotionKey) AS distinct_keys
FROM RAW_DIMPROMOTION;

-- Validation: null / blank checks
SELECT
  SUM(CASE WHEN PromotionKey IS NULL THEN 1 ELSE 0 END) AS null_key_rows,
  SUM(CASE WHEN PromotionName IS NULL OR TRIM(PromotionName) = '' THEN 1 ELSE 0 END) AS blank_name_rows
FROM RAW_DIMPROMOTION;

-- Sample check
SELECT *
FROM RAW_DIMPROMOTION
ORDER BY PromotionKey
LIMIT 20;

-- Task 2.2.4
-- =========================================================
-- Context
-- =========================================================
USE ROLE TRAINING_ROLE;
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- =========================================================
-- Step 1: Confirm file exists in stage
-- =========================================================
LIST @CONTOSO_STAGE;

-- =========================================================
-- Step 2: Preview staged data
-- Header already skipped by CONTOSO_FILE_FORMAT
-- =========================================================
SELECT
  $1  AS DateKey,
  $2  AS CalendarYear,
  $3  AS CalendarYearLabel,
  $4  AS CalendarHalfYearLabel,
  $5  AS CalendarQuarterLabel,
  $6  AS CalendarMonthLabel,
  $7  AS CalendarWeekLabel,
  $8  AS CalendarDayOfWeekLabel,
  $9  AS FiscalYear,
  $10 AS FiscalYearLabel,
  $11 AS FiscalHalfYearLabel,
  $12 AS FiscalQuarterLabel,
  $13 AS FiscalMonthLabel,
  $14 AS IsWorkDay,
  $15 AS IsHoliday,
  $16 AS EuropeSeason,
  $17 AS NorthAmericaSeason,
  $18 AS AsiaSeason,
  $19 AS MonthNumber,
  $20 AS CalendarDayOfWeekNumber
FROM @CONTOSO_STAGE/DimDate.csv
LIMIT 10;

-- =========================================================
-- Step 3: Create RAW table (matches CSV headers exactly)
-- =========================================================
CREATE OR REPLACE TABLE RAW_DIMDATE (
  DateKey                    DATE,
  CalendarYear               INTEGER,
  CalendarYearLabel           VARCHAR,
  CalendarHalfYearLabel       VARCHAR,
  CalendarQuarterLabel        VARCHAR,
  CalendarMonthLabel          VARCHAR,
  CalendarWeekLabel           VARCHAR,
  CalendarDayOfWeekLabel      VARCHAR,
  FiscalYear                 INTEGER,
  FiscalYearLabel             VARCHAR,
  FiscalHalfYearLabel         VARCHAR,
  FiscalQuarterLabel          VARCHAR,
  FiscalMonthLabel            VARCHAR,
  IsWorkDay                  VARCHAR,
  IsHoliday                  VARCHAR,
  EuropeSeason               VARCHAR,
  NorthAmericaSeason         VARCHAR,
  AsiaSeason                 VARCHAR,
  MonthNumber                INTEGER,
  CalendarDayOfWeekNumber    INTEGER
);


-- =========================================================
-- Step 4: Load data
-- =========================================================
COPY INTO RAW_DIMDATE
FROM @CONTOSO_STAGE/DimDate.csv
FILE_FORMAT = (FORMAT_NAME = CONTOSO_FILE_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- =========================================================
-- Step 5: Validation - row count
-- =========================================================
SELECT COUNT(*) AS total_rows
FROM RAW_DIMDATE;

-- =========================================================
-- Step 6: Validation - DateKey uniqueness
-- =========================================================
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT DateKey) AS distinct_dates
FROM RAW_DIMDATE;


-- =========================================================
-- Step 7: Validation – null checks (should be zero)
-- =========================================================
SELECT
  SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS null_datekey,
  SUM(CASE WHEN CalendarYear IS NULL THEN 1 ELSE 0 END) AS null_calendaryear
FROM RAW_DIMDATE;


-- Step 8: Quick sample

SELECT *
FROM RAW_DIMDATE
ORDER BY DateKey
LIMIT 20;


-- Task 2.3.1
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.1: Create Initial View for Store Sales
CREATE OR REPLACE VIEW VIEW_MASTER_SALES AS
SELECT 
    -- IDs
    f.SalesKey,
    f.DateKey,
    f.StoreKey,
    f.ProductKey,
    
    -- Metrics
    f.SalesQuantity,
    f.UnitPrice,
    
    -- Dimensions
    s.StoreName,
    g.RegionCountryName,
    p.ProductName,
    p.BrandName,
    d.CalendarYear,
    
    -- Employee (Store Manager)
    (emp.FirstName || ' ' || emp.LastName) AS StoreManagerName

FROM RAW_CONTOSO.RAW_FACTSALES f

-- Join to Main Dimensions
LEFT JOIN RAW_CONTOSO.RAW_DIMSTORE s 
    ON f.StoreKey = s.StoreKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCT p 
    ON f.ProductKey = p.ProductKey
LEFT JOIN RAW_CONTOSO.RAW_DIMDATE d 
    ON f.DateKey = d.DateKey
LEFT JOIN RAW_CONTOSO.RAW_DIMCHANNEL c 
    ON f.channelKey = c.channelKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPROMOTION pro 
    ON f.PromotionKey = pro.PromotionKey

-- Join to Snowflaked Dimensions (Linked via other tables)
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g 
    ON s.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMEMPLOYEE emp 
    ON s.StoreManager = emp.EmployeeKey;



USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.1: Create Initial View for Online Sales
CREATE OR REPLACE VIEW VIEW_MASTER_ONLINESALES AS
SELECT 
    -- IDs
    f.OnlineSalesKey,
    f.DateKey,
    f.CustomerKey,
    f.ProductKey,
    
    -- Metrics
    f.SalesQuantity,
    f.UnitPrice,
    
    -- Dimensions
    (cust.FirstName || ' ' || cust.LastName) AS CustomerName,
    g.RegionCountryName,
    p.ProductName,
    p.BrandName,
    d.CalendarYear,
    cur.CurrencyName

FROM RAW_CONTOSO.RAW_FACT_ONLINESALES f

-- Join to Main Dimensions
LEFT JOIN RAW_CONTOSO.RAW_DIMCUSTOMER cust 
    ON f.CustomerKey = cust.CustomerKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCT p 
    ON f.ProductKey = p.ProductKey
LEFT JOIN RAW_CONTOSO.RAW_DIMDATE d 
    ON f.DateKey = d.DateKey
LEFT JOIN RAW_CONTOSO.RAW_DIMCURRENCY cur 
    ON f.CurrencyKey = cur.CurrencyKey

-- Join to Snowflaked Dimensions
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g 
    ON cust.GeographyKey = g.GeographyKey;

-- Task 2.3.2
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.2: Switch to INNER JOIN (Filter Orphans)
CREATE OR REPLACE VIEW VIEW_MASTER_SALES AS
SELECT 
    f.SalesKey,
    f.DateKey,
    f.StoreKey,
    f.ProductKey,
    
    f.SalesQuantity,
    f.UnitPrice,
    
    s.StoreName,
    g.RegionCountryName,
    p.ProductName,
    p.BrandName,
    d.CalendarYear,
    
    (emp.FirstName || ' ' || emp.LastName) AS StoreManagerName

FROM RAW_CONTOSO.RAW_FACTSALES f

-- [CRITICAL CHANGE] INNER JOIN removes rows with invalid Keys
INNER JOIN RAW_CONTOSO.RAW_DIMSTORE s 
    ON f.StoreKey = s.StoreKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p 
    ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d 
    ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCHANNEL c 
    ON f.channelKey = c.channelKey
INNER JOIN RAW_CONTOSO.RAW_DIMPROMOTION pro 
    ON f.PromotionKey = pro.PromotionKey

-- [KEEP LEFT JOIN] These are linked to Dimensions, not the Fact table directly
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g 
    ON s.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMEMPLOYEE emp 
    ON s.StoreManager = emp.EmployeeKey;


USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.2: Switch to INNER JOIN (Filter Orphans)
CREATE OR REPLACE VIEW VIEW_MASTER_ONLINESALES AS
SELECT 
    f.OnlineSalesKey,
    f.DateKey,
    f.CustomerKey,
    f.ProductKey,
    
    f.SalesQuantity,
    f.UnitPrice,
    
    (cust.FirstName || ' ' || cust.LastName) AS CustomerName,
    g.RegionCountryName,
    p.ProductName,
    p.BrandName,
    d.CalendarYear,
    cur.CurrencyName

FROM RAW_CONTOSO.RAW_FACT_ONLINESALES f

-- [CRITICAL CHANGE] INNER JOIN removes rows with invalid Keys
INNER JOIN RAW_CONTOSO.RAW_DIMCUSTOMER cust 
    ON f.CustomerKey = cust.CustomerKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p 
    ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d 
    ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCURRENCY cur 
    ON f.CurrencyKey = cur.CurrencyKey

-- [KEEP LEFT JOIN] Geography is linked to Customer
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g 
    ON cust.GeographyKey = g.GeographyKey;

-- Task 2.3.3
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.3: Rename Columns & Add Hierarchy
CREATE OR REPLACE VIEW VIEW_MASTER_SALES AS
SELECT 
    -- IDs (Keep these for technical use)
    f.SalesKey,
    f.DateKey,
    
    -- Metrics
    f.SalesQuantity,
    f.UnitPrice,
    
    -- [NEW] Business Friendly Names
    s.StoreName                   AS Store,            -- Simplified Name
    s.StoreType,
    g.RegionCountryName           AS Country,          -- Renamed
    g.ContinentName               AS Continent,        -- Renamed
    
    p.ProductName                 AS Product,
    p.BrandName                   AS Brand,
    cat.ProductCategoryName       AS Category,         -- Joined & Renamed
    sub.ProductSubcategoryName    AS SubCategory,      -- Joined & Renamed
    
    d.CalendarYear                AS Year,             -- Simplified
    
    -- Store Manager Name
    (emp.FirstName || ' ' || emp.LastName) AS StoreManagerName

FROM RAW_CONTOSO.RAW_FACTSALES f

-- Inner Joins (Task 2.3.2 Logic)
INNER JOIN RAW_CONTOSO.RAW_DIMSTORE s ON f.StoreKey = s.StoreKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCHANNEL c ON f.channelKey = c.channelKey
INNER JOIN RAW_CONTOSO.RAW_DIMPROMOTION pro ON f.PromotionKey = pro.PromotionKey

-- Snowflaked Joins (Added SubCategory & Category here)
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON s.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMEMPLOYEE emp ON s.StoreManager = emp.EmployeeKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey;


USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.3: Rename Columns & Add Hierarchy
CREATE OR REPLACE VIEW VIEW_MASTER_ONLINESALES AS
SELECT 
    f.OnlineSalesKey,
    f.DateKey,
    
    -- Metrics
    f.SalesQuantity,
    f.UnitPrice,
    
    -- [NEW] Business Friendly Names
    (cust.FirstName || ' ' || cust.LastName) AS CustomerName,
    g.RegionCountryName           AS Country,          -- Renamed
    g.ContinentName               AS Continent,
    
    p.ProductName                 AS Product,
    p.BrandName                   AS Brand,
    cat.ProductCategoryName       AS Category,         -- Joined & Renamed
    sub.ProductSubcategoryName    AS SubCategory,      -- Joined & Renamed
    
    d.CalendarYear                AS Year,
    cur.CurrencyName

FROM RAW_CONTOSO.RAW_FACT_ONLINESALES f

-- Inner Joins (Task 2.3.2 Logic)
INNER JOIN RAW_CONTOSO.RAW_DIMCUSTOMER cust ON f.CustomerKey = cust.CustomerKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCURRENCY cur ON f.CurrencyKey = cur.CurrencyKey

-- Snowflaked Joins (Added SubCategory & Category here)
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON cust.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey;

-- Task 2.3.4
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.4: Final Master View with Revenue Calculation
CREATE OR REPLACE VIEW VIEW_MASTER_SALES AS
SELECT 
    f.SalesKey,
    f.DateKey,
    
    -- [NEW] TASK 2.3.4: THE CALCULATION
    f.SalesQuantity,
    f.UnitPrice,
    (f.SalesQuantity * f.UnitPrice) AS TotalRevenue,   -- Quantity * Price
    
    -- Business Friendly Dimensions
    s.StoreName                   AS Store,
    s.StoreType,
    g.RegionCountryName           AS Country,
    g.ContinentName               AS Continent,
    
    p.ProductName                 AS Product,
    p.BrandName                   AS Brand,
    cat.ProductCategoryName       AS Category,
    sub.ProductSubcategoryName    AS SubCategory,
    
    d.CalendarYear                AS Year,
    
    (emp.FirstName || ' ' || emp.LastName) AS StoreManagerName

FROM RAW_CONTOSO.RAW_FACTSALES f

-- Inner Joins (Data Quality)
INNER JOIN RAW_CONTOSO.RAW_DIMSTORE s ON f.StoreKey = s.StoreKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCHANNEL c ON f.channelKey = c.channelKey
INNER JOIN RAW_CONTOSO.RAW_DIMPROMOTION pro ON f.PromotionKey = pro.PromotionKey

-- Snowflaked Joins
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON s.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMEMPLOYEE emp ON s.StoreManager = emp.EmployeeKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey;

USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 2.3.4: Final Master View with Revenue Calculation
CREATE OR REPLACE VIEW VIEW_MASTER_ONLINESALES AS
SELECT 
    f.OnlineSalesKey,
    f.DateKey,
    
    -- [NEW] TASK 2.3.4: THE CALCULATION
    f.SalesQuantity,
    f.UnitPrice,
    (f.SalesQuantity * f.UnitPrice) AS TotalRevenue,   -- Quantity * Price
    
    -- Business Friendly Dimensions
    (cust.FirstName || ' ' || cust.LastName) AS CustomerName,
    g.RegionCountryName           AS Country,
    g.ContinentName               AS Continent,
    
    p.ProductName                 AS Product,
    p.BrandName                   AS Brand,
    cat.ProductCategoryName       AS Category,
    sub.ProductSubcategoryName    AS SubCategory,
    
    d.CalendarYear                AS Year,
    cur.CurrencyName

FROM RAW_CONTOSO.RAW_FACT_ONLINESALES f

-- Inner Joins (Data Quality)
INNER JOIN RAW_CONTOSO.RAW_DIMCUSTOMER cust ON f.CustomerKey = cust.CustomerKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCURRENCY cur ON f.CurrencyKey = cur.CurrencyKey

-- Snowflaked Joins
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON cust.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey;

-- Task 2.4.1
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO; 

SELECT DISTINCT RegionCountryName, GeographyKey
FROM RAW_CONTOSO.RAW_DIMGEOGRAPHY
WHERE RegionCountryName LIKE '%Sing%' 
   OR RegionCountryName LIKE '%Malay%'
   OR RegionCountryName LIKE '%Thai%'
   OR RegionCountryName LIKE '%Viet%';
   
-- Task 2.4.2
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

CREATE OR REPLACE VIEW VIEW_SEA_PERFORMANCE AS
SELECT *
FROM DW_CONTOSO.VIEW_MASTER_SALES
WHERE Country IN ('Singapore', 'Malaysia', 'Thailand', 'Vietnam');

CREATE OR REPLACE VIEW VIEW_SEA_ONLINE_PERFORMANCE AS
SELECT *
FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES
WHERE Country IN ('Singapore', 'Malaysia', 'Thailand', 'Vietnam');

SELECT * FROM VIEW_SEA_PERFORMANCE LIMIT 100;
SELECT * FROM VIEW_SEA_ONLINE_PERFORMANCE LIMIT 100;

-- Task 2.4.3
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO; 

SELECT COUNT(*) AS Non_SEA_Rows
FROM VIEW_SEA_PERFORMANCE
WHERE Country NOT IN ('Singapore', 'Malaysia', 'Thailand', 'Vietnam');

-- Task 2.5.1
-- 1. Check for Products sold that don't exist in DimProduct Offline
SELECT 
    'Orphan Product Keys Offline' AS Issue_Type, 
    COUNT(*) AS Fail_Count,
FROM RAW_FACTSALES f
LEFT JOIN RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL

UNION ALL

-- 2. Check for Products sold that don't exist in DimProduct Online
SELECT 
    'Orphan Product Keys Online' AS Issue_Type, 
    COUNT(*) AS Fail_Count,
FROM RAW_FACT_ONLINESALES f
LEFT JOIN RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL

UNION ALL

-- 3. Check for Stores that don't exist in DimStore
SELECT 
    'Orphan Store Keys' AS Issue_Type, 
    COUNT(*) AS Fail_Count,
FROM RAW_FACTSALES f
LEFT JOIN RAW_DIMSTORE s ON f.StoreKey = s.StoreKey
WHERE s.StoreKey IS NULL

UNION ALL

-- 4. Check for Dates that don't exist in DimDate
SELECT 
    'Orphan Date Keys' AS Issue_Type, 
    COUNT(*) AS Fail_Count,
FROM RAW_FACTSALES f
LEFT JOIN RAW_DIMDATE d ON f.DateKey = d.Datekey 
WHERE d.Datekey IS NULL;

-- Task 2.5.2
-- 1. Create the Task (Schedule: 0th Minute, 0th Hour (Midnight) in Singapore Time)
CREATE OR REPLACE TASK LOAD_FACTSALES_DAILY_TASK
  WAREHOUSE = 'CHIPMUNK_WH' 
  SCHEDULE = 'USING CRON 0 0 * * * Asia/Singapore' 
AS
  COPY INTO RAW_FACTSALES
  FROM @CONTOSO_STAGE/FactSales.csv.gz
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1)
  ON_ERROR = 'CONTINUE';

-- 2. "Turn On" the Task (Tasks are created in 'Suspended' state by default)
ALTER TASK LOAD_FACTSALES_DAILY_TASK RESUME;

-- 3. Verification: Check that the task is currently 'STARTED'
SHOW TASKS LIKE 'LOAD_FACTSALES_DAILY_TASK';

-- Task 2.5.3
SELECT 
    TABLE_NAME, 
    ROW_COUNT,
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW_CONTOSO'
ORDER BY TABLE_NAME;

-- Sprint 3
-- Task 3.1.1
USE DATABASE CONTOSO_DB;
USE SCHEMA RAW_CONTOSO;

-- TASK 3.1.1: INVENTORY OF PLACEHOLDERS (RAW DATA PROFILE)
-- Updated to be CASE-INSENSITIVE and include EMPTY STRINGS/SPACES.

-- 1. Shared Dimension: PRODUCT
SELECT 
    'RAW_DIMPRODUCT' AS Source_Table,
    'BrandName'      AS Column_Name,
    'Shared (Online & Store)' AS Channel_Usage,
    -- Case Insensitive Check for N/A
    SUM(CASE WHEN UPPER(BrandName) = 'N/A' THEN 1 ELSE 0 END)     AS Count_NA,
    -- Case Insensitive Check for Unknown
    SUM(CASE WHEN UPPER(BrandName) = 'UNKNOWN' THEN 1 ELSE 0 END) AS Count_Unknown,
    -- Check for Empty Strings or Just Spaces
    SUM(CASE WHEN TRIM(BrandName) = '' THEN 1 ELSE 0 END)         AS Count_Empty,
    -- Standard NULL Check
    SUM(CASE WHEN BrandName IS NULL THEN 1 ELSE 0 END)            AS Count_NULL
FROM RAW_DIMPRODUCT

UNION ALL

-- 2. Physical Sales Dimension: STORE
SELECT 
    'RAW_DIMSTORE',
    'StoreType',
    'Physical Sales Only',
    SUM(CASE WHEN UPPER(StoreType) = 'N/A' THEN 1 ELSE 0 END),
    SUM(CASE WHEN UPPER(StoreType) = 'UNKNOWN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN TRIM(StoreType) = '' THEN 1 ELSE 0 END),
    SUM(CASE WHEN StoreType IS NULL THEN 1 ELSE 0 END)
FROM RAW_DIMSTORE

UNION ALL

-- 3. Online Sales Dimension: CUSTOMER
SELECT 
    'RAW_DIMCUSTOMER',
    'Education',
    'Online Sales Only',
    SUM(CASE WHEN UPPER(Education) = 'N/A' THEN 1 ELSE 0 END),
    SUM(CASE WHEN UPPER(Education) = 'UNKNOWN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN TRIM(Education) = '' THEN 1 ELSE 0 END),
    SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END)
FROM RAW_DIMCUSTOMER;



USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;


-- Proving that all case variations of "N/A" and "Unknown" are gone.

-- 1. Check Physical Sales View
SELECT 
    'VIEW_MASTER_SALES' AS Reporting_View,
    'Brand'             AS Column_Name,
    SUM(CASE WHEN UPPER(Brand) = 'N/A' THEN 1 ELSE 0 END)     AS Count_NA,      -- Should be 0
    SUM(CASE WHEN UPPER(Brand) = 'UNKNOWN' THEN 1 ELSE 0 END) AS Count_Unknown, -- Should be 0
    SUM(CASE WHEN TRIM(Brand) = '' THEN 1 ELSE 0 END)         AS Count_Empty,   -- Should be 0
    SUM(CASE WHEN Brand IS NULL THEN 1 ELSE 0 END)            AS Count_Clean_NULL
FROM VIEW_MASTER_SALES

UNION ALL

-- 2. Check Online Sales View
SELECT 
    'VIEW_MASTER_ONLINESALES' AS Reporting_View,
    'Brand'                   AS Column_Name,
    SUM(CASE WHEN UPPER(Brand) = 'N/A' THEN 1 ELSE 0 END)     AS Count_NA,
    SUM(CASE WHEN UPPER(Brand) = 'UNKNOWN' THEN 1 ELSE 0 END) AS Count_Unknown,
    SUM(CASE WHEN TRIM(Brand) = '' THEN 1 ELSE 0 END)         AS Count_Empty,
    SUM(CASE WHEN Brand IS NULL THEN 1 ELSE 0 END)            AS Count_Clean_NULL
FROM VIEW_MASTER_ONLINESALES;

-- Task 3.1.2
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

SELECT TOP 100
    s.StoreName AS Original_Value,

    CASE 
        WHEN TRIM(s.StoreName) = '' THEN NULL
        WHEN LOWER(TRIM(s.StoreName)) IN ('n/a', 'unknown', 'null', '0') THEN NULL
        
        ELSE TRIM(s.StoreName)
    END AS Cleaned_Value,

    CASE 
        WHEN TRIM(s.StoreName) = '' THEN 'Empty String Rule'
        WHEN LOWER(TRIM(s.StoreName)) IN ('n/a', 'unknown', 'null', '0') THEN 'Placeholder Rule'
        ELSE 'Valid Data'
    END AS Rule_Applied

FROM RAW_CONTOSO.RAW_DIMSTORE s

WHERE s.StoreName IN ('', ' ', 'N/A', '0', 'Unknown') 
   OR s.Status IN ('N/A', '0');

-- Task 3.2.2
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- 3.2.2 DUPLICATE DETECTION SUITE (FIXED)
-- Validating uniqueness for Sales, Online Sales, Products, AND Customers.

WITH Duplicate_Check_CTE AS (
    -- 1. Store Sales Check
    SELECT 
        'Store Sales' AS Entity, 
        'SalesKey' AS Criteria,
        (SELECT COUNT(*) FROM (
            SELECT SalesKey FROM VIEW_MASTER_SALES GROUP BY SalesKey HAVING COUNT(*) > 1
        )) AS Cnt

    UNION ALL

    -- 2. Online Sales Check
    SELECT 
        'Online Sales', 
        'OnlineSalesKey',
        (SELECT COUNT(*) FROM (
            SELECT OnlineSalesKey FROM VIEW_MASTER_ONLINESALES GROUP BY OnlineSalesKey HAVING COUNT(*) > 1
        ))

    UNION ALL

    -- 3. Product Check
    SELECT 
        'Product', 
        'ProductKey',
        (SELECT COUNT(*) FROM (
            SELECT ProductKey FROM RAW_CONTOSO.RAW_DIMPRODUCT GROUP BY ProductKey HAVING COUNT(*) > 1
        ))

    UNION ALL

    -- 4. Customer Check (Required for feedback)
    SELECT 
        'Customer', 
        'CustomerKey',
        (SELECT COUNT(*) FROM (
            SELECT CustomerKey FROM RAW_CONTOSO.RAW_DIMCUSTOMER GROUP BY CustomerKey HAVING COUNT(*) > 1
        ))
)
-- Final Output: Now we can safely use the counts calculated above
SELECT 
    Entity,
    Criteria,
    Cnt AS Duplicate_Count,
    CASE 
        WHEN Cnt = 0 THEN 'PASSED (No Examples Available)' 
        ELSE 'FAILED' 
    END AS Status
FROM Duplicate_Check_CTE;

-- Task 3.2.3
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- TASK 3.2.3: Apply Deduplication Logic (Defensive View)
-- Strategy: "Keep Latest" based on Date/ID.

CREATE OR REPLACE VIEW VIEW_CLEAN_SALES_DATA AS
WITH RankedSales AS (
    SELECT 
        *,
        -- Assign a rank: 1 = The "Best" row. 2,3,etc = Duplicates.
        ROW_NUMBER() OVER (
            PARTITION BY SalesKey                -- Group by the Unique ID
            ORDER BY DateKey DESC                -- Keep the most recent data
        ) AS RowRank
    FROM DW_CONTOSO.VIEW_MASTER_SALES
)
SELECT * FROM RankedSales
WHERE RowRank = 1; -- <--- This filter removes the duplicates automatically

-- Repeat for Online Sales
CREATE OR REPLACE VIEW VIEW_CLEAN_ONLINESALES_DATA AS
WITH RankedOnline AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY OnlineSalesKey          -- Group by the Unique ID
            ORDER BY DateKey DESC
        ) AS RowRank
    FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES
)
SELECT * FROM RankedOnline
WHERE RowRank = 1;


-- The "Before vs After" Check
SELECT 
    'Original (Master)'       AS View_Type, 
    COUNT(*)                  AS Row_Count 
FROM DW_CONTOSO.VIEW_MASTER_SALES

UNION ALL

SELECT 
    'Cleaned (Deduplicated)'  AS View_Type, 
    COUNT(*)                  AS Row_Count 
FROM DW_CONTOSO.VIEW_CLEAN_SALES_DATA;

-- Task 3.2.4
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- 1. Validate Store Sales (Should match 'SalesKey')
WITH Validation AS (
    SELECT SalesKey, COUNT(*) 
    FROM VIEW_CLEAN_SALES_DATA  -- <--- Checking the Clean View
    GROUP BY SalesKey
    HAVING COUNT(*) > 1
)
SELECT 
    'VIEW_CLEAN_SALES_DATA' AS Checked_View,
    'SalesKey' AS Criteria,
    (SELECT COUNT(*) FROM Validation) AS Duplicate_Count,
    CASE 
        WHEN (SELECT COUNT(*) FROM Validation) = 0 THEN 'PASSED (0 Duplicates)'
        ELSE 'FAILED'
    END AS Status;

-- 2. Validate Online Sales (Should match 'OnlineSalesKey')
WITH Validation AS (
    SELECT OnlineSalesKey, COUNT(*) 
    FROM VIEW_CLEAN_ONLINESALES_DATA -- <--- Checking the Clean View
    GROUP BY OnlineSalesKey
    HAVING COUNT(*) > 1
)
SELECT 
    'VIEW_CLEAN_ONLINESALES_DATA' AS Checked_View,
    'OnlineSalesKey' AS Criteria,
    (SELECT COUNT(*) FROM Validation) AS Duplicate_Count,
    CASE 
        WHEN (SELECT COUNT(*) FROM Validation) = 0 THEN 'PASSED (0 Duplicates)'
        ELSE 'FAILED'
    END AS Status;

-- Task 3.3.2
-- Expected Results: Raw Count should equal View Count (or View count is slightly lower since we cleaned data).
-- FYI, if View Count > Raw Count, could mean that have a critical error.

SELECT 
    'Physical Sales' AS Source,
    (SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACTSALES) AS Raw_Count,
    (SELECT COUNT(*) FROM DW_CONTOSO.VIEW_MASTER_SALES) AS View_Count,
    (SELECT COUNT(*) FROM DW_CONTOSO.VIEW_MASTER_SALES) - (SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACTSALES) AS Difference
UNION ALL
SELECT 
    'Online Sales' AS Source,
    (SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACT_ONLINESALES) AS Raw_Count,
    (SELECT COUNT(*) FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES) AS View_Count,
    (SELECT COUNT(*) FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES) - (SELECT COUNT(*) FROM RAW_CONTOSO.RAW_FACT_ONLINESALES) AS Difference;

-- Task 3.3.3
-- Expected Result: 0 Rows returned. 
-- If rows are returned, it means these Keys appear more than once (Bad Data).

SELECT 'Physical Sales Duplicates' AS Test_Type, SalesKey AS Key_ID, COUNT(*) AS Count
FROM DW_CONTOSO.VIEW_MASTER_SALES
GROUP BY SalesKey
HAVING COUNT(*) > 1

UNION ALL

SELECT 'Online Sales Duplicates' AS Test_Type, OnlineSalesKey AS Key_ID, COUNT(*) AS Count
FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES
GROUP BY OnlineSalesKey
HAVING COUNT(*) > 1;

-- Task 3.3.4
-- Goal: Ensure no financial value was lost or doubled during transformation.

SELECT 
    'Physical Sales' AS Dataset,
    (SELECT SUM(SalesQuantity * UnitPrice) FROM RAW_CONTOSO.RAW_FACTSALES) AS Raw_Revenue_Calc,
    (SELECT SUM(TotalRevenue) FROM DW_CONTOSO.VIEW_MASTER_SALES) AS View_Revenue_Total,
    (SELECT SUM(TotalRevenue) FROM DW_CONTOSO.VIEW_MASTER_SALES) - 
    (SELECT SUM(SalesQuantity * UnitPrice) FROM RAW_CONTOSO.RAW_FACTSALES) AS Difference
UNION ALL
SELECT 
    'Online Sales' AS Dataset,
    (SELECT SUM(SalesQuantity * UnitPrice) FROM RAW_CONTOSO.RAW_FACT_ONLINESALES) AS Raw_Revenue_Calc,
    (SELECT SUM(TotalRevenue) FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES) AS View_Revenue_Total,
    (SELECT SUM(TotalRevenue) FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES) - 
    (SELECT SUM(SalesQuantity * UnitPrice) FROM RAW_CONTOSO.RAW_FACT_ONLINESALES) AS Difference;

-- Task 3.3.5
-- "THE RECONCILIATION BRIDGE"

SELECT 
    'Raw Source For Physical Sales' AS Category, 
    SUM(SalesQuantity * UnitPrice) AS Revenue 
FROM RAW_CONTOSO.RAW_FACTSALES

UNION ALL

SELECT 
    'Physical: Less Orphans (Invalid Products)' AS Category, 
    -SUM(SalesQuantity * UnitPrice) AS Revenue 
FROM RAW_CONTOSO.RAW_FACTSALES
WHERE ProductKey NOT IN (SELECT ProductKey FROM RAW_CONTOSO.RAW_DIMPRODUCT)
   OR StoreKey NOT IN (SELECT StoreKey FROM RAW_CONTOSO.RAW_DIMSTORE)

UNION ALL

SELECT 
    'Final View For Physical Sales (Actual)' AS Category, 
    SUM(TotalRevenue) AS Revenue 
FROM DW_CONTOSO.VIEW_MASTER_SALES;


-- 2. ONLINE SALES RECONCILIATION
SELECT 
    'Raw Source For Online Sales' AS Category, 
    SUM(SalesQuantity * UnitPrice) AS Revenue 
FROM RAW_CONTOSO.RAW_FACT_ONLINESALES
UNION ALL
SELECT 
    'Online: Less Orphans (Invalid Products)' AS Category, 
    -SUM(SalesQuantity * UnitPrice) AS Revenue 
FROM RAW_CONTOSO.RAW_FACT_ONLINESALES
WHERE ProductKey NOT IN (SELECT ProductKey FROM RAW_CONTOSO.RAW_DIMPRODUCT)
UNION ALL
SELECT 
    'Final View For Online Sales (Actual)' AS Category, 
    SUM(TotalRevenue) AS Revenue 
FROM DW_CONTOSO.VIEW_MASTER_ONLINESALES;

-- Task 3.4.2 and 3.4.3
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- =============================================================================
-- FINAL REPORTING VIEW: PHYSICAL STORE SALES
-- Task 3.4.2 & Task 3.4.3
-- =============================================================================
CREATE OR REPLACE VIEW VIEW_RPT_SALES_MASTER AS
SELECT 

    f.SalesKey,
    f.DateKey,
    f.StoreKey,
    f.ProductKey,


    f.SalesQuantity,
    f.UnitPrice,
    f.UnitCost,
    (f.SalesQuantity * f.UnitPrice) AS TotalRevenue,
    (f.SalesQuantity * f.UnitCost)  AS TotalCost,
    ((f.SalesQuantity * f.UnitPrice) - (f.SalesQuantity * f.UnitCost)) AS NetProfit,

    NULLIF(TRIM(s.StoreName), '') AS StoreName,
    CASE 
        WHEN s.Status IN ('N/A', 'Unknown', 'Off', '0') THEN NULL 
        ELSE s.Status 
    END AS StoreStatus,
    s.StoreType,

    
    TRIM(g.RegionCountryName)     AS CountryName,
    TRIM(g.ContinentName)         AS ContinentName,


    TRIM(p.ProductName)           AS ProductName,
    TRIM(p.BrandName)             AS BrandName,
    TRIM(cat.ProductCategoryName) AS CategoryName,
    TRIM(sub.ProductSubcategoryName) AS SubCategoryName,
    

    TRIM(emp.FirstName || ' ' || emp.LastName) AS StoreManagerName,

    d.CalendarYear                AS CalendarYear

FROM RAW_CONTOSO.RAW_FACTSALES f
INNER JOIN RAW_CONTOSO.RAW_DIMSTORE s ON f.StoreKey = s.StoreKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON s.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMEMPLOYEE emp ON s.StoreManager = emp.EmployeeKey;


-- =============================================================================
-- FINAL REPORTING VIEW: ONLINE SALES
-- Task 3.4.2 & Task 3.4.3
-- =============================================================================
CREATE OR REPLACE VIEW VIEW_RPT_ONLINESALES_MASTER AS
SELECT 
    f.OnlineSalesKey AS SalesKey,
    f.DateKey,
    f.CustomerKey,
    f.ProductKey,

    f.SalesQuantity,
    f.UnitPrice,
    f.UnitCost,
    (f.SalesQuantity * f.UnitPrice) AS TotalRevenue,
    (f.SalesQuantity * f.UnitCost)  AS TotalCost,
    ((f.SalesQuantity * f.UnitPrice) - (f.SalesQuantity * f.UnitCost)) AS NetProfit,

    TRIM(cust.FirstName || ' ' || cust.LastName) AS CustomerName,
    CASE 
        WHEN cust.Gender IN ('N/A', 'Unknown', '0') THEN NULL 
        ELSE cust.Gender 
    END AS Gender,

    TRIM(g.RegionCountryName)     AS CountryName,
    TRIM(g.ContinentName)         AS ContinentName,

    TRIM(p.ProductName)           AS ProductName,
    TRIM(p.BrandName)             AS BrandName,
    TRIM(cat.ProductCategoryName) AS CategoryName,
    
    d.CalendarYear                AS CalendarYear,
    cur.CurrencyName              AS CurrencyName

FROM RAW_CONTOSO.RAW_FACT_ONLINESALES f
INNER JOIN RAW_CONTOSO.RAW_DIMCUSTOMER cust ON f.CustomerKey = cust.CustomerKey
INNER JOIN RAW_CONTOSO.RAW_DIMPRODUCT p ON f.ProductKey = p.ProductKey
INNER JOIN RAW_CONTOSO.RAW_DIMDATE d ON f.DateKey = d.DateKey
INNER JOIN RAW_CONTOSO.RAW_DIMCURRENCY cur ON f.CurrencyKey = cur.CurrencyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMGEOGRAPHY g ON cust.GeographyKey = g.GeographyKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTSUBCATEGORY sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN RAW_CONTOSO.RAW_DIMPRODUCTCATEGORY cat ON sub.ProductCategoryKey = cat.ProductCategoryKey;

-- Task 3.4.4
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

-- =============================================================================
-- TASK 3.4.4: VALIDATION SUITE FOR PHYSICAL SALES
-- =============================================================================

-- Row Count &  Filter Verification
SELECT 'Raw Fact Table' AS Source, COUNT(*) AS Row_Count FROM RAW_CONTOSO.RAW_FACTSALES
UNION ALL
SELECT 'Final Reporting View', COUNT(*) FROM DW_CONTOSO.VIEW_RPT_SALES_MASTER;

-- CHECK 2: Cleaning Logic Verification
SELECT COUNT(*) AS Failed_Cleaning_Rows
FROM DW_CONTOSO.VIEW_RPT_SALES_MASTER
WHERE StoreName = '' OR StoreName = 'N/A' 
   OR StoreStatus = 'N/A' OR StoreStatus = 'Unknown';

-- CHECK 3: Metric Integrity (Math Check)
SELECT COUNT(*) AS Missing_Revenue_Rows
FROM DW_CONTOSO.VIEW_RPT_SALES_MASTER
WHERE TotalRevenue IS NULL;

-- CHECK 4: Primary Key Duplication Check (Fan-out Check)
SELECT SalesKey, COUNT(*) 
FROM DW_CONTOSO.VIEW_RPT_SALES_MASTER
GROUP BY SalesKey
HAVING COUNT(*) > 1;

-- =============================================================================
-- TASK 3.4.4: VALIDATION SUITE FOR ONLINE SALES
-- =============================================================================

-- CHECK 1: Row Count Verification
SELECT 'Raw Online Table' AS Source, COUNT(*) AS Row_Count FROM RAW_CONTOSO.RAW_FACT_ONLINESALES
UNION ALL
SELECT 'Final Online View', COUNT(*) FROM DW_CONTOSO.VIEW_RPT_ONLINESALES_MASTER;

-- CHECK 2: Cleaning Logic Verification
SELECT COUNT(*) AS Failed_Cleaning_Rows
FROM DW_CONTOSO.VIEW_RPT_ONLINESALES_MASTER
WHERE Gender IN ('N/A', 'Unknown', '0')
   OR CustomerName = '';

-- CHECK 3: Metric Integrity
SELECT COUNT(*) AS Missing_Revenue_Rows
FROM DW_CONTOSO.VIEW_RPT_ONLINESALES_MASTER
WHERE TotalRevenue IS NULL;

-- CHECK 4: Duplication Check
SELECT SalesKey, COUNT(*) 
FROM DW_CONTOSO.VIEW_RPT_ONLINESALES_MASTER
GROUP BY SalesKey
HAVING COUNT(*) > 1;

-- Task 3.6.1
USE DATABASE CONTOSO_DB;
USE SCHEMA DW_CONTOSO;

SELECT * FROM DW_CONTOSO.VIEW_RPT_SALES_MASTER;

SELECT * FROM RAW_CONTOSO.RAW_DIMDATE;

SELECT * FROM DW_CONTOSO.VIEW_RPT_ONLINESALES_MASTER;