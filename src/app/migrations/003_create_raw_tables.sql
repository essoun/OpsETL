IF OBJECT_ID('dbo.raw_orders','U') IS NULL
BEGIN
    CREATE TABLE dbo.raw_orders (
        id            INT IDENTITY(1,1) PRIMARY KEY,
        payload_json   NVARCHAR(MAX) NOT NULL,
        ingested_at    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO
