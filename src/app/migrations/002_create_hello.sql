IF OBJECT_ID('dbo.hello','U') IS NULL
BEGIN
    CREATE TABLE dbo.hello (
        id INT IDENTITY(1,1) PRIMARY KEY,
        msg NVARCHAR(100) NOT NULL
    );
END
GO
