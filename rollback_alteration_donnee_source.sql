UPDATE [SalesLT].[Address] SET CountryRegion = 'Canada' WHERE CountryRegion = 'Kanada';

UPDATE [SalesLT].[Customer] SET Title = 'Mr.' WHERE Title = 'Monsieur';

UPDATE [SalesLT].[CustomerAddress] SET AddressType = 'Main Office' WHERE AddressType = 'Second Office';

UPDATE [SalesLT].[Product] SET Color = 'Black' WHERE Color = 'Noir';

UPDATE [SalesLT].[ProductCategory] SET Name = 'Gloves' WHERE Name = 'Gants';

UPDATE [SalesLT].[ProductDescription] SET Description = 'Chromoly steel.' WHERE Description = 'Description alteree';

UPDATE [SalesLT].[ProductModel] SET Name = 'LL Road Frame' WHERE Name = 'Nouveau model';

UPDATE [SalesLT].[ProductModelProductDescription] SET Culture = 'en' WHERE Culture = 'eng';

UPDATE [SalesLT].[SalesOrderDetail] SET OrderQty = 1 WHERE OrderQty = 10;

UPDATE [SalesLT].[SalesOrderHeader] SET PurchaseOrderNumber = 'PO19952192051' WHERE PurchaseOrderNumber = 'PO1995219999';