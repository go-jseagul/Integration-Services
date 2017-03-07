-- =============================================
-- Author:		Ryan Fant
-- Create date: 09/26/2014
-- Description:	Retrieves next adwords account
--				report queue item to download
--				from the adwords api
--
-- Mod By:		Ryan Fant
-- Mod Date:	01/28/2015
-- Description:	Added ReportTypes support
-- =============================================  
CREATE PROCEDURE [dbo].[uspReportQueue_Deque]
(
	-- if any output value is null then there was no queue item to process
	@rptlogId BIGINT OUTPUT
	,@clientId BIGINT OUTPUT
	,@rptdDate DATE OUTPUT
	,@reportTypes INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY

		DECLARE @rptqId INT;

		BEGIN TRAN;

			-- get next item in queue
			-- using the cte creates an atomic read/update transaction
			-- this is different from derived tables which can spool before an update is applied
			WITH rptQueue AS
			(
				SELECT TOP 1
					q.[rptqId]
					,q.[rptdDate]
					,q.[clientId]
					,q.[rptqReportTypes]
					,q.[rptqRunning]
					,q.[rptqAttemptsRemaining]
				-- table hints & filtered, covering index (IX_ReportQueue_queue) required to support highly concurrent dequeuing
				-- this approach is used instead of changing the transaction isolation level and locking the table for serial access 
				-- this approach locks the index for update but allows concurrent threads to read the next candidate
				FROM [dbo].[ReportQueue] q WITH (INDEX(IX_ReportQueue_queue), UPDLOCK, READPAST)
				WHERE
					-- to keep high concurrency simple we don't bother looking at the Client table
					-- since the insertion process to the ReportQueue honors the clientEnabled flag
					-- if there is a desire to disable all ReportQueue entries for a given clientId
					-- then this table must be manually updated as well 
					q.[rptqEnabled] = 1
					AND q.[rptqRunning] = 0
					AND q.[rptqComplete] = 0
					AND q.[rptqAttemptsRemaining] > 0
				ORDER BY q.[rptqCreatedOn]
			)
			UPDATE rptQueue
			SET
				rptQueue.[rptqRunning] = 1
				,rptQueue.[rptqAttemptsRemaining] = rptQueue.[rptqAttemptsRemaining] - 1
				,@rptqId = rptQueue.[rptqId]
				,@clientId = rptQueue.[clientId]
				,@rptdDate = rptQueue.[rptdDate]
				,@reportTypes = rptQueue.[rptqReportTypes];

			IF @rptqId IS NOT NULL -- we have a record to process
			BEGIN

				IF @clientId IS NULL
				BEGIN
					;THROW 50000, '@clientId is ''NULL''; failed to properly read ReporQueue record', 1;
				END;

				IF @rptdDate IS NULL
				BEGIN
					;THROW 50000, '@rptdDate is ''NULL''; failed to properly read ReporQueue record', 1;
				END;

				-- create the log entry
				INSERT [dbo].[ReportLog] ( [rptqId] , [rptlogReportTypes] ) VALUES ( @rptqId , @reportTypes );

				SET @rptlogId = SCOPE_IDENTITY();

				-- update the queue with the new current log entry
				UPDATE q
				SET
					q.[rptqCurrentLog_rptlogId] = @rptlogId
					,q.[rptqLastUpdatedOn] = SYSUTCDATETIME() -- don't need a var since this is a single row update
				FROM [dbo].[ReportQueue] q
				WHERE q.[rptqId] = @rptqId;

				IF @rptlogId IS NULL
				BEGIN
					;THROW 50000, '@rptlogId is ''NULL''; failed to create ReporLog record', 1;
				END;

			END;

		COMMIT TRAN;

	END TRY
	BEGIN CATCH

		IF 0 != XACT_STATE()
		BEGIN
			ROLLBACK TRAN;
		END;

		THROW;

	END CATCH;

END;
