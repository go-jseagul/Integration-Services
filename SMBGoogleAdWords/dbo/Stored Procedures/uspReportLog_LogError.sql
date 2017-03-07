-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/28/2014
-- Description:	updates log entry with errlog id
--				also updates the queue entry
-- =============================================  
CREATE PROCEDURE [dbo].[uspReportLog_LogError]
(
       @rptlogId BIGINT
	   ,@errlogId BIGINT
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		IF @rptlogId IS NULL
		BEGIN
			;THROW 50000, '@rptlogId cannot be ''NULL''', 1;
		END;

		IF @errlogId IS NULL
		BEGIN
			;THROW 50000, '@errlogId cannot be ''NULL''', 1;
		END;

		DECLARE
			@rptqId BIGINT 
			,@utcNow DATETIME2(7) = SYSUTCDATETIME();

		UPDATE [dbo].[ReportLog]
		SET
			@rptqId = [rptqId]
			,[rptlogLastUpdatedOn] = @utcNow
			,[errlogId] = @errlogId
		WHERE [rptlogId] = @rptlogId;

		UPDATE [dbo].[ReportQueue]
		SET
			[rptqRunning] = 0
			,[rptqComplete] = 0
			,[rptqLastUpdatedOn] = @utcNow
		WHERE [rptqId] = @rptqId;

	END TRY
	BEGIN CATCH

		THROW;

	END CATCH;

END;
