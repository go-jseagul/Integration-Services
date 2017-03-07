-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/28/2014
-- Description:	updates log entry as zero impr.
--				also marks the queue entry complete
-- =============================================  
CREATE PROCEDURE [dbo].[uspReportLog_SetZeroImpressions]
(
       @rptlogId BIGINT
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		IF @rptlogId IS NULL
		BEGIN
			;THROW 50000, '@rptlogId cannot be ''NULL''', 1;
		END;

		DECLARE
			@rptqId BIGINT 
			,@utcNow DATETIME2(7) = SYSUTCDATETIME();

		UPDATE [dbo].[ReportLog]
		SET
			@rptqId = [rptqId]
			,[rptlogLastUpdatedOn] = @utcNow
			,[rptlogCompletedOn] = @utcNow
			,[acctperfZeroImpressions] = 1
		WHERE [rptlogId] = @rptlogId;

		UPDATE [dbo].[ReportQueue]
		SET
			[rptqRunning] = 0
			,[rptqComplete] = 1
			,[rptqLastSucessfulLog_rptlogId] = @rptlogId
			,[rptqLastUpdatedOn] = @utcNow
		WHERE [rptqId] = @rptqId;

	END TRY
	BEGIN CATCH

		THROW;

	END CATCH;

END;
