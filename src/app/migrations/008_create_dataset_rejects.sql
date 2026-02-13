IF OBJECT_ID('dbo.dataset_rejects','U') IS NULL
BEGIN
    CREATE TABLE dbo.dataset_rejects (
        reject_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        dataset_name    NVARCHAR(100) NOT NULL,
        source_file     NVARCHAR(500) NULL,
        row_num         INT NOT NULL,
        row_hash        VARBINARY(32) NOT NULL,
        reject_reasons  NVARCHAR(1000) NOT NULL,
        raw_json        NVARCHAR(MAX) NOT NULL,
        created_at      DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_dataset_rejects_dataset_created
        ON dbo.dataset_rejects(dataset_name, created_at);
END
GO
