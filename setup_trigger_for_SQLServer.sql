drop table Customer_master;

create table Customer_master
(
	Customer_id integer primary key,
	Customer_name varchar(200),
	Customer_type varchar(12)
);

create table Customer_detail
(
	Customer_id integer,
	Detail_id integer,
	Phone_number varchar(20)
	constraint pk_cust_dtl primary key (Customer_id, Detail_id)
);


-- First, create a CDC audit table to store complete row changes
DROP TABLE IF EXISTS dbo.CDC_Row_Audit;

CREATE TABLE dbo.CDC_Row_Audit (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    TableName VARCHAR(100) NOT NULL,
    OperationType CHAR(1) NOT NULL, -- 'I' for Insert, 'U' for Update, 'D' for Delete
	OperationKey Varchar(32) NOT NULL, -- Key based on table & Combining PKs 
	OperationTimeKey Varchar(32) NOT NULL, -- Key based on table & Combining PKs & Datetime2
    PK1 varchar(128) NOT NULL,          -- 1st Primary key of the affected record
    PK2 varchar(128) NULL,          -- 2nd Primary key of the affected record
    PK3 varchar(128) NULL,          -- 3rd Primary key of the affected record
    PK4 varchar(128) NULL,          -- 4th Primary key of the affected record
    PK5 varchar(128) NULL,          -- 5th Primary key of the affected record
    PK6 varchar(128) NULL,          -- 6th Primary key of the affected record
    PK7 varchar(128) NULL,          -- 7th Primary key of the affected record
    PK8 varchar(128) NULL,          -- 8th Primary key of the affected record
    PK9 varchar(128) NULL,          -- 9th Primary key of the affected record
    BeforeImage NVARCHAR(MAX) NULL, -- Complete row JSON data before change (for updates and deletes)
    AfterImage NVARCHAR(MAX) NULL,  -- Complete row JSON data after change (for inserts and updates)
	valid_data varchar(1) Not Null DEFAULT 'Y',
	processed_data varchar(1) not null DEFAULT 'N',
	applied_data varchar(1) not null DEFAULT 'N', 
    ChangeDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ChangedBy VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER
);


/*
    SET @combined_string = @table_name + '|' + @key_value + '|' + CONVERT(NVARCHAR(30), @timestamp, 126);
    
    RETURN CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', @combined_string), 2);
		USE MD5 instead of SHA2_256 to match to Oracle 11g
*/

-- Create the CDC trigger that captures entire row changes on the target table
CREATE OR ALTER TRIGGER trg_CDC_Row_Customer_Master
ON dbo.Customer_Master
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName VARCHAR(100) = 'Customer_Master';
    DECLARE @ChangeDate DATETIME2 = GETDATE();
--	DECLARE @combined_string VARCHAR(1148);
--	DECLARE @combined_timestamp_string VARCHAR(1280);
    
    -- Handle INSERT operations - store complete new row
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType, OperationKey, OperationTimeKey, PK1, AfterImage, ChangeDate)
        SELECT 
            @TableName,
            'I',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar)), 2) OperationKey,
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(i.Customer_ID as varchar) PK1,
            (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
			@ChangeDate
        FROM inserted i;
    END
    
    -- Handle DELETE operations - store complete deleted row
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType,  OperationKey, OperationTimeKey, PK1, BeforeImage, ChangeDate)
        SELECT 
            @TableName,
            'D',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(d.Customer_ID as varchar)), 2) OperationKey,
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(d.Customer_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(d.Customer_ID as varchar) PK1,
            (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
			@ChangeDate
        FROM deleted d;
    END
    
    -- Handle UPDATE operations - store both before and after row images
    ELSE IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType,  OperationKey, OperationTimeKey, PK1, BeforeImage, AfterImage, ChangeDate)
        SELECT 
            @TableName,
            'U',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar)), 2) OperationKey, -- Oracle 11g is not supporting SHA2_256 so best use MD5
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(i.Customer_ID as varchar) PK1,
            (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
			@ChangeDate
        FROM inserted i
        JOIN deleted d ON i.Customer_ID = d.Customer_ID;
    END
END;


-- Create the CDC trigger that captures entire row changes on the target table
CREATE OR ALTER TRIGGER trg_CDC_Row_Customer_Detail
ON dbo.Customer_Detail
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName VARCHAR(100) = 'Customer_Detail';
    DECLARE @ChangeDate DATETIME2 = GETDATE();
    
    -- Handle INSERT operations - store complete new row
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType,  OperationKey, OperationTimeKey, PK1, PK2, AfterImage)
        SELECT 
            @TableName,
            'I',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + cast(i.Detail_ID as varchar)), 2) OperationKey,
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + cast(i.Detail_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(i.Customer_ID as varchar) PK1,
            cast(i.Detail_ID as varchar) PK2,
            (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    END
    
    -- Handle DELETE operations - store complete deleted row
    ELSE IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType, OperationKey, OperationTimeKey, PK1, PK2, BeforeImage)
        SELECT 
            @TableName,
            'D',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(d.Customer_ID as varchar) + '|' + cast(d.Detail_ID as varchar)), 2) OperationKey,
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(d.Customer_ID as varchar) + '|' + cast(d.Detail_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(d.Customer_ID as varchar) PK1,
            cast(d.Detail_ID as varchar) PK2,
            (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
    END
    
    -- Handle UPDATE operations - store both before and after row images
    ELSE IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO dbo.CDC_Row_Audit (TableName, OperationType, OperationKey, OperationTimeKey, PK1, PK2, BeforeImage, AfterImage)
        SELECT 
            @TableName,
            'U',
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + cast(i.Detail_ID as varchar)), 2) OperationKey,
			CONVERT(NVARCHAR(32), HASHBYTES('MD5', @TableName + '|' + cast(i.Customer_ID as varchar) + '|' + cast(i.Detail_ID as varchar) + '|' + CONVERT(NVARCHAR(30), @ChangeDate, 126)), 2) OperationTimeKey,
            cast(i.Customer_ID as varchar) PK1,
            cast(i.Detail_ID as varchar) PK2,
            (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i
        JOIN deleted d ON i.Customer_ID = d.Customer_ID;
    END
END;


-- Test data
Begin
	insert into customer_detail (customer_id, detail_id, Phone_number) values (1001,1, '+91 98410 11160'), (1001,2, '+971 58 272 4870'), (1001,3, '+65 84641866'), (1001,4, '+1-818-414-8231')
	insert into customer_master values (1001, 'Srini Ramaswamy', 'T1')
end

	UPDATE customer_detail 
			set Phone_number = '+91 (98410) 11160'
	where
		customer_id = 1001
	and
		detail_id = 1
;

select * from CDC_ROW_Audit;
select * from Customer_master;
select * from Customer_detail;

