-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/28/2014
-- Description:	logs application error and
--				returns error log id
-- =============================================  
CREATE PROCEDURE [dbo].[uspErrorLog_Insert]
(
	@errlogText VARCHAR(MAX)
	,@errlogCategory VARCHAR(255)
	,@errlogId BIGINT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		IF @errlogText IS NULL
		BEGIN
			;THROW 50000, '@errlogText cannot be ''NULL''', 1;
		END;

		IF @errlogCategory IS NULL
		BEGIN
			;THROW 50000, '@errlogCategory cannot be ''NULL''', 1;
		END;

		INSERT [dbo].[ErrorLog] ( [errlogMachineName] , [errlogUserLogin] , [errlogCategory] , [errlogText] )
		VALUES ( HOST_NAME() , SYSTEM_USER , @errlogCategory , @errlogText );

		SET @errlogId = SCOPE_IDENTITY();

	END TRY
	BEGIN CATCH

		THROW;

	END CATCH;

END;
