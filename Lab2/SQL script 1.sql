--Task 2.4

CREATE EXTERNAL TABLE All2019Sales (
    [TransactionId] varchar(4000) COLLATE Latin1_General_100_BIN2_UTF8,
    [CustomerId] int,
    [ProductId] smallint,
    [Quantity] smallint,
    [Price] numeric(38,18),
    [TotalAmount] numeric(38,18),  
    [TransactionDate] int,
    [ProfitAmount] numeric(38,18),
    [Hour] smallint,
    [Minute] smallint,
    [StoreId] smallint
    )
    WITH (
    LOCATION = 'sale-small/Year=2019/*/*/*/*.parquet',
    DATA_SOURCE = [wwi-02_asadatalakeic6nl8d_dfs_core_windows_net],
    FILE_FORMAT = [SynapseParquetFormat]
    )
GO



--Task 3.1

IF NOT EXISTS (SELECT * FROM sys.symmetric_keys) BEGIN
    declare @pasword nvarchar(400) = CAST(newid() as VARCHAR(400));
    EXEC('CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @pasword + '''')
END

CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'sv=2018-03-28&ss=bf&srt=sco&sp=rl&st=2019-10-14T12%3A10%3A25Z&se=2061-12-31T12%3A10%3A00Z&sig=KlSU2ullCscyTS0An0nozEpo4tO5JAgGBvw%2FJX2lguw%3D'
GO

-- Create external data source secured using credential
CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH (
    LOCATION = 'https://sqlondemandstorage.blob.core.windows.net',
    CREDENTIAL = sqlondemand
);
GO

CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeader
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);
GO

CREATE EXTERNAL TABLE [population]
(
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2,
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2,
    [year] smallint,
    [population] bigint
)
WITH (
    LOCATION = 'csv/population/population.csv',
    DATA_SOURCE = SqlOnDemandDemo,
    FILE_FORMAT = QuotedCsvWithHeader
);
GO


--Task 3.3

SELECT [country_code]
    ,[country_name]
    ,[year]
    ,[population]
FROM [dbo].[population]
WHERE [year] = 2019 and population > 100000000

--Task 4.3

CREATE VIEW CustomerInfo AS
    SELECT * 
FROM OPENROWSET(
        BULK 'https://asadatalakeic6nl8d.dfs.core.windows.net/wwi-02/customer-info/customerinfo.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        FIRSTROW=2
    )
    WITH (
    [UserName] NVARCHAR (50),
    [Gender] NVARCHAR (10),
    [Phone] NVARCHAR (50),
    [Email] NVARCHAR (100),
    [CreditCard] NVARCHAR (50)
    ) AS [r];
    GO

SELECT * FROM CustomerInfo;
GO





