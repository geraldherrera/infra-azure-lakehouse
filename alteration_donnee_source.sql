UPDATE [SalesLT].[Address] SET CountryRegion = 'Kanada' WHERE CountryRegion = 'Canada';

UPDATE [SalesLT].[Customer] SET Title = 'Monsieur' WHERE Title = 'Mr.';

UPDATE [SalesLT].[CustomerAddress] SET AddressType = 'Second Office' WHERE AddressType = 'Main Office';

UPDATE [SalesLT].[Product] SET Color = 'Noir' WHERE Color = 'Black';

UPDATE [SalesLT].[ProductCategory] SET Name = 'Gants' WHERE Name = 'Gloves';

UPDATE [SalesLT].[ProductDescription] SET Description = 'Description alteree' WHERE Description = 'Chromoly steel.';

UPDATE [SalesLT].[ProductModel] SET Name = 'Nouveau model' WHERE Name = 'LL Road Frame';

UPDATE [SalesLT].[ProductModelProductDescription] SET Culture = 'eng' WHERE Culture = 'en';

UPDATE [SalesLT].[SalesOrderDetail] SET OrderQty = 10 WHERE OrderQty = 1;

UPDATE [SalesLT].[SalesOrderHeader] SET PurchaseOrderNumber = 'PO1995219999' WHERE PurchaseOrderNumber = 'PO19952192051';