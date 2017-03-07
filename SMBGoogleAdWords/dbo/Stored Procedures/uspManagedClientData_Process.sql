-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/26/2014
-- Description:	Takes raw xml managed client
--				data from the AdWords api and
--				updates the Client table
-- =============================================  
CREATE PROCEDURE [dbo].[uspManagedClientData_Process]
(
	-- TODO: bind to schema to ensure proper xml structure and protect against api versioning problems
	@managedCustomerPageXml XML
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		IF @managedCustomerPageXml IS NULL
		BEGIN
			;THROW 50000, '@managedCustomerPageXml cannot be ''NULL''', 1;
		END;

		/*
			<ManagedCustomerPage xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
				...
				<entries xmlns="https://adwords.google.com/api/adwords/mcm/v201406">
				<name>1100 Broadway House Account</name>
				<companyName>1100 Broadway/Tennessean</companyName>
				<customerId>2807729027</customerId>
				<canManageClients>false</canManageClients>
				<currencyCode>USD</currencyCode>
				<dateTimeZone>America/Chicago</dateTimeZone>
				<testAccount>false</testAccount>
				</entries>
				...
			</ManagedCustomerPage>
		*/

		IF EXISTS ( SELECT * FROM sys.indexes WHERE OBJECT_ID = OBJECT_ID('raw.ManagedCustomer') AND NAME ='IX_ManagedCustomer' )
		BEGIN
			DROP INDEX [IX_ManagedCustomer] ON [raw].[ManagedCustomer];
		END;

		TRUNCATE TABLE [raw].[ManagedCustomer];
		
		INSERT [raw].[ManagedCustomer]
		(
			[name]
			,[companyName]
			,[customerId]
			,[canManageClients]
			,[currencyCode]
			,[dateTimeZone]
			,[testAccount]
		)
		SELECT
			NULLIF(t.c.value('(*:name)[1]','varchar(500)'),'') name
			,NULLIF(t.c.value('(*:companyName)[1]','varchar(500)'),'') companyName
			,NULLIF(t.c.value('(*:customerId)[1]','bigint'),'') customerId
			,NULLIF(t.c.value('(*:canManageClients)[1]','bit'),'') canManageClients
			,NULLIF(t.c.value('(*:currencyCode)[1]','varchar(50)'),'') currencyCode
			,NULLIF(t.c.value('(*:dateTimeZone)[1]','varchar(50)'),'') dateTimeZone
			,NULLIF(t.c.value('(*:testAccount)[1]','bit'),'') testAccount
		FROM @managedCustomerPageXml.nodes('*:ManagedCustomerPage/*:entries') t(c);	
		
		CREATE NONCLUSTERED INDEX [IX_ManagedCustomer]
		ON [raw].[ManagedCustomer] ( [canManageClients] , [testAccount] )
		INCLUDE ( [customerId] );

		/*
			<ManagedCustomerPage xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
			  ...
			  <links xmlns="https://adwords.google.com/api/adwords/mcm/v201406">
				<managerCustomerId>5980437010</managerCustomerId>
				<clientCustomerId>5577681102</clientCustomerId>
			  </links>
			  ...
			</ManagedCustomerPage>
		*/
		
		-- TODO: use links to determine hierarchies to exlude (i.e. dev/inactive MCCs)
		TRUNCATE TABLE [raw].[ManagedCustomerLink];

		INSERT [raw].[ManagedCustomerLink]
		(
			[managerCustomerId]
			,[clientCustomerId]
		)
		SELECT
			NULLIF(t.c.value('(*:managerCustomerId)[1]','bigint'),'') managerCustomerId
			,NULLIF(t.c.value('(*:clientCustomerId)[1]','bigint'),'') clientCustomerId
		FROM @managedCustomerPageXml.nodes('*:ManagedCustomerPage/*:links') t(c);

		-- prefer static time stamps for all records in given execution
		-- also improves performance by not executing a function for each row
		DECLARE @utcNow DATETIME2(7) = SYSUTCDATETIME();

		-- we back off two days just to be safe since this could run late in the day 
		-- which utc would be tomorrow and stats we would be looking for would be yesterday
		-- this is approximate and could be enhanced in the future with additional api calls
		DECLARE @startDate DATE = DATEADD ( DAY , -2 , @utcNow );

		-- TODO: separate to another proc so raw load and transform can be run separately
		-- TODO: disable [dbo].[Client] index(es) and rebuild after merge or create an appropriate maintenance plan

		MERGE [dbo].[Client] c
		USING
		(
			SELECT [customerId]
			FROM [raw].[ManagedCustomer]
			WHERE
				-- client managers are not client accounts and won't have activity
				( [canManageClients] IS NULL OR [canManageClients] = 0 )
				-- test accounts should be excluded (intelligent hierarchy processing should negate this constraint)
				AND ( [testAccount] IS NULL OR [testAccount] = 0 )
		)
		AS src
		(
			[customerId]
		)
		ON 
		(
			c.[clientId] = src.[customerId]
		)
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				[clientId]
				,[clientStartDate]
				,[clientEnabled]
			)
			VALUES
			(
				src.[customerId] -- clientId (adwords account identifier)
				,@startDate -- clientStartDate (first date stats should exist)
				,1 -- clientEnabled (optimistically enrolling all new accounts for download)
			)
		WHEN MATCHED THEN
			UPDATE
			SET
				c.[clientLastFoundOn] = @utcNow -- provides visibility to freshness of data
		;

	END TRY
	BEGIN CATCH

		THROW;

	END CATCH;

END;
