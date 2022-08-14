USE master
GO

IF DB_ID (N'Profiles') IS NULL
BEGIN
    CREATE DATABASE Profiles;
END
GO

USE Profiles
GO

DROP VIEW IF EXISTS OnlineUserProfile01;
GO

CREATE VIEW OnlineUserProfile01
AS
SELECT
    *
FROM OPENROWSET(
    'CosmosDB',
    N'account=asacosmosdbic6nl8d;database=CustomerProfile;key=fcoamMvZbSANO5ye63lEIYv3W03IBKzwP9ZqSqOSYTMuhIEualUQ7pzHX7WjMbkgI7iQZQgg6FMwbMLK05NSiQ==',
    UserProfileHTAP
    --OnlineUserProfile01
)
WITH (
    userId bigint,
    cartId varchar(50),
    preferredProducts varchar(max),
    productReviews varchar(max)
) AS profiles
CROSS APPLY OPENJSON (productReviews)
WITH (
    productId bigint,
    reviewText varchar(1000)
) AS reviews
GO