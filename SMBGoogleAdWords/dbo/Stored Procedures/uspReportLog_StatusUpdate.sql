-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/28/2014
-- Description:	updates progress for provided
--				rptlogId, also updates error id
--				if an error occurred in the processor
--
-- Mod By:		Ryan Fant
-- Mod Date:	01/28/2015
-- Description:	Added complete flag support to
--				enable partial report type
--				downloads

-- Mod By:		Seagul
-- Mod Date:	02/28/2017
-- Description:	Added Age,Gender and Video report types

-- =============================================  
CREATE PROCEDURE [dbo].[uspReportLog_StatusUpdate]
(
       @rptlogId BIGINT
	   ,@reportType VARCHAR(50)
	   ,@reportStage VARCHAR(50)	   
	   ,@complete BIT
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		DECLARE @errMsg NVARCHAR(255);

		IF @rptlogId IS NULL
		BEGIN
			;THROW 50000, '@rptlogId cannot be ''NULL''', 1;
		END;

		IF @reportType IS NULL
		BEGIN
			;THROW 50000, '@reportType cannot be ''NULL''', 1;
		END;

		IF @reportType NOT IN 
			(
				'Account'
				,'Campaign'
				,'AdGroup'
				,'Ad'
				,'Keyword'
				,'Age'
				,'Gender'
				,'Video'
			)
		BEGIN
			SET @errMsg = 'reportType:[' + @reportType + '] not supported';
			;THROW 50000, @errMsg, 1;
		END;

		IF @reportStage IS NULL
		BEGIN
			;THROW 50000, '@reportStage cannot be ''NULL''', 1;
		END;

		IF @reportStage NOT IN
			(
				'ReportRequested'
				,'Downloaded'
				,'Decompressed'
				,'LoadStarted'
				,'LoadCompleted'
			)
		BEGIN
			SET @errMsg = 'reportStage:[' + @reportStage + '] not supported';
			;THROW 50000, @errMsg, 1;
		END;

		IF @complete IS NULL
		BEGIN
			;THROW 50000, '@complete cannot be ''NULL''', 1;
		END;

		DECLARE
			@rptqId INT
			,@utcNow DATETIME2(7) = SYSUTCDATETIME();
		
		IF @reportType = 'Account'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[acctperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[acctperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[acctperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[acctperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[acctperfLoadCompletedOn] = @utcNow
					,[acctperfZeroImpressions] = 0
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END
		ELSE IF @reportType = 'Campaign'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[campperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[campperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[campperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[campperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[campperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END
		ELSE IF @reportType = 'AdGroup'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adgrpperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adgrpperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adgrpperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adgrpperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[adgrpperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END
		ELSE IF @reportType = 'Ad'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[adperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[adperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END
		ELSE IF @reportType = 'Keyword'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END;
		ELSE IF @reportType = 'Age'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END;
		ELSE IF @reportType = 'Gender'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END;
		ELSE IF @reportType = 'Video'
		BEGIN
			IF @reportStage = 'ReportRequested'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfReportRequestedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE IF @reportStage = 'Downloaded'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDownloadedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'Decompressed'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfDecompressedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadStarted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadStartedOn] = @utcNow
				WHERE rptlogId = @rptlogId;
			END
			ELSE 
			IF @reportStage = 'LoadCompleted'
			BEGIN
				UPDATE [dbo].[ReportLog]
				SET
					@rptqId = [rptqId]
					,[rptlogLastUpdatedOn] = @utcNow
					,[kwperfLoadCompletedOn] = @utcNow
					,[rptlogCompletedOn] = CASE WHEN @complete = 1 THEN @utcNow ELSE NULL END
				WHERE rptlogId = @rptlogId;
			END;
		END;
		IF @complete = 1
		BEGIN
			UPDATE [dbo].[ReportQueue]
			SET
				[rptqRunning] = 0
				,[rptqComplete] = 1
				,[rptqLastSucessfulLog_rptlogId] = @rptlogId
				,[rptqLastUpdatedOn] = @utcNow
			WHERE [rptqId] = @rptqId;
		END;

	END TRY
	BEGIN CATCH

		THROW;

	END CATCH;

END;
