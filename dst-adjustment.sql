USE [¿]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[some_dst_proc]
	@adhoc_exec INT = NULL,
	@dst_type VARCHAR(50) = NULL,
	@debug BIT = NULL
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @err_msg VARCHAR(1000);

	IF (LOWER(@dst_type) NOT IN ('spring forward', 'fall back'))
	BEGIN
		SET @err_msg = 'Oh no! @dst_type contains an incorrect value (' + @dst_type + '). Values accepted are: ''spring forward'' and ''fall back''.'
		GOTO stopExec
	END

	DECLARE @sSQL VARCHAR(MAX), @job_name SYSNAME, @job_id UNIQUEIDENTIFIER, @schedule_id INT, @active_start_time INT, @active_end_time INT, @ss_freq_subday_type INT 
	DECLARE @scope_identity INT
	DECLARE @active_start_time_adjusted TIME, @active_start_time_adjusted_int INT, @active_end_time_adjusted TIME, @active_end_time_adjusted_int INT

	IF (@debug = 0 OR @debug IS NULL)
	BEGIN	
		IF (SELECT [name] FROM sys.tables WHERE [name] = 'some_dst_log') IS NULL
		BEGIN
			CREATE TABLE dbo.some_dst_log (
				[id] INT IDENTITY(1,1) NOT NULL,
				[run_date] DATETIME NOT NULL,
				[job_name] SYSNAME NOT NULL,
				[job_id] UNIQUEIDENTIFIER NOT NULL,
				[ss_freq_subday_type] INT NOT NULL,
				[active_start_time_before] INT NOT NULL,
				[active_start_time_adjusted] INT NULL,
				[active_end_time_before] INT NULL,
				[active_end_time_adjusted] INT NULL
			)
		END

		--	error handling for March/November within the same month/year more than once.
		IF (
			MONTH(GETDATE()) = 3 AND 
			NOT EXISTS (SELECT TOP (1) [id] FROM dbo.some_dst_log WHERE MONTH(run_date) = 3 AND YEAR(run_date) = YEAR(GETDATE()))
		)
		BEGIN
			SET @dst_type = 'spring forward'
		END
		ELSE IF (
			MONTH(GETDATE()) = 3 AND
			EXISTS (SELECT TOP (1) [id] FROM dbo.some_dst_log WHERE MONTH(run_date) = 3 AND YEAR(run_date) = YEAR(GETDATE()))
		)
		BEGIN
			SET @err_msg = 'March''s DST job has already been ran this year! Use the following query to check the log table (and clear out records if required).
			SELECT * FROM dbo.some_dst_log WHERE [run_date] >= DATEADD(MONTH, -1, GETDATE())' 
			GOTO stopExec
		END

		IF (
			MONTH(GETDATE()) = 11 AND 
			NOT EXISTS (SELECT TOP (1) [id] FROM dbo.some_dst_log WHERE MONTH(run_date) = 11 AND YEAR(run_date) = YEAR(GETDATE()))
		)
		BEGIN
			SET @dst_type = 'fall back'
		END
		ELSE IF (
			MONTH(GETDATE()) = 11 AND
			EXISTS (SELECT TOP (1) [id] FROM dbo.some_dst_log WHERE MONTH(run_date) = 11 AND YEAR(run_date) = YEAR(GETDATE()))
		)
		BEGIN
			SET @err_msg = 'November''s DST job has already been ran this year! Use the following query to check the log table (and clear out records <^> if required <V>).
			SELECT * FROM dbo.some_dst_log WHERE [run_date] >= DATEADD(MONTH, -1, GETDATE())' 
			GOTO stopExec
		END
	
		--	freq_subday_type 0 - Schedule , 1 - At the specified time, 2 - Seconds, 4 - Minutes, 8 - Hours
		IF (DATEPART(HOUR, SYSUTCDATETIME()) - DATEPART(HOUR, SYSDATETIME())) = 0
		BEGIN
			DECLARE cur_dst CURSOR FOR 
				SELECT 
					MAX(j.name) AS [j_name], 
					MAX(j.job_id) AS [j_id],
					s.schedule_id,
					MAX(ss.active_start_time) AS [start_time],
					MAX(ss.active_end_time) AS [end_time],
					MAX(ss.freq_subday_type) AS [subday]
				FROM msdb.dbo.sysjobs j
				INNER JOIN msdb.dbo.sysjobschedules s ON s.job_id=j.job_id
				INNER JOIN msdb.dbo.sysschedules ss ON ss.schedule_id=s.schedule_id
				WHERE 
					ss.freq_subday_type IN (0, 1, 8) 
				GROUP BY s.schedule_id

			OPEN cur_dst
				FETCH cur_dst INTO @job_name, @job_id, @schedule_id, @active_start_time, @active_end_time, @ss_freq_subday_type
				WHILE @@FETCH_STATUS <> - 1
				BEGIN
					INSERT INTO dbo.some_dst_log (run_date, job_name, job_id, ss_freq_subday_type, active_start_time_before, active_end_time_before)
					SELECT GETDATE(), @job_name, @job_id, @ss_freq_subday_type, @active_start_time, @active_end_time
			
					SET @scope_identity = (SELECT  SCOPE_IDENTITY())

					IF (@dst_type = 'spring forward')
					BEGIN
						SET @active_start_time_adjusted = (SELECT CAST(DATEADD(HOUR, -1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_start_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_start_time_adjusted_int = CAST(REPLACE(@active_start_time_adjusted, ':', '') AS DECIMAL)

						SET @active_end_time_adjusted = (SELECT CAST(DATEADD(HOUR, -1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_end_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_end_time_adjusted_int = CAST(REPLACE(@active_end_time_adjusted, ':', '') AS DECIMAL)

						IF (@active_start_time = 0)
						BEGIN
							SET @active_start_time_adjusted_int = 230000
						END
						IF (@active_end_time = 5959)
						BEGIN
							SET @active_end_time_adjusted_int = 235959
						END
					END

					IF (@dst_type = 'fall back')
					BEGIN
						SET @active_start_time_adjusted = (SELECT CAST(DATEADD(HOUR, 1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_start_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_start_time_adjusted_int = CAST(REPLACE(@active_start_time_adjusted, ':', '') AS DECIMAL)

						SET @active_end_time_adjusted = (SELECT CAST(DATEADD(HOUR, 1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_end_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_end_time_adjusted_int = CAST(REPLACE(@active_end_time_adjusted, ':', '') AS DECIMAL)

						IF (@active_start_time = 0)
						BEGIN					
							SET @active_start_time_adjusted_int = 10000
						END

						IF (@active_end_time = 235959)
						BEGIN
							SET @active_end_time_adjusted_int = 5959
						END
					END

					IF (@ss_freq_subday_type IN (0,1))
					BEGIN
						EXEC msdb.dbo.sp_update_schedule @schedule_id = @schedule_id, @active_start_time = @active_start_time_adjusted_int

						UPDATE dbo.some_dst_log
						SET 
							active_start_time_adjusted = @active_start_time_adjusted_int
						WHERE
							id = @scope_identity
					END

					IF (@ss_freq_subday_type NOT IN (0,1))
					BEGIN
						EXEC msdb.dbo.sp_update_schedule @schedule_id = @schedule_id, @active_start_time = @active_start_time_adjusted_int, @active_end_time = @active_end_time_adjusted_int

						UPDATE dbo.some_dst_log
						SET 
							active_start_time_adjusted = @active_start_time_adjusted_int,
							active_end_time_adjusted = @active_end_time_adjusted_int
						WHERE
							id = @scope_identity
					END

				FETCH NEXT FROM cur_dst INTO @job_name, @job_id, @schedule_id, @active_start_time, @active_end_time, @ss_freq_subday_type
				END
			CLOSE cur_dst
			DEALLOCATE cur_dst
		END	
	END
	ELSE
	--	@debug = 1
	BEGIN
		CREATE TABLE #dst_debug (
			[id] INT IDENTITY(1,1) NOT NULL,
			[run_date] DATETIME NOT NULL,
			[job_name] SYSNAME NOT NULL,
			[job_id] UNIQUEIDENTIFIER NOT NULL,
			[ss_freq_subday_type] INT NOT NULL,
			[active_start_time_before] INT NOT NULL,
			[active_start_time_adjusted] INT NULL,
			[active_end_time_before] INT NULL,
			[active_end_time_adjusted] INT NULL
		)

		IF (DATEPART(HOUR, SYSUTCDATETIME()) - DATEPART(HOUR, SYSDATETIME())) = 0
		BEGIN
			DECLARE cur_dst CURSOR FOR 
				SELECT 
					MAX(j.name) AS [j_name], 
					MAX(j.job_id) AS [j_id],
					s.schedule_id,
					MAX(ss.active_start_time) AS [start_time],
					MAX(ss.active_end_time) AS [end_time],
					MAX(ss.freq_subday_type) AS [subday]
				FROM msdb.dbo.sysjobs j
				INNER JOIN msdb.dbo.sysjobschedules s ON s.job_id=j.job_id
				INNER JOIN msdb.dbo.sysschedules ss ON ss.schedule_id=s.schedule_id
				WHERE 
					ss.freq_subday_type IN (0, 1, 8) 
				GROUP BY s.schedule_id

			OPEN cur_dst
				FETCH cur_dst INTO @job_name, @job_id, @schedule_id, @active_start_time, @active_end_time, @ss_freq_subday_type
				WHILE @@FETCH_STATUS <> - 1
				BEGIN
					INSERT INTO #dst_debug (run_date, job_name, job_id, ss_freq_subday_type, active_start_time_before, active_end_time_before)
					SELECT GETDATE(), @job_name, @job_id, @ss_freq_subday_type, @active_start_time, @active_end_time
			
					SET @scope_identity = (SELECT  SCOPE_IDENTITY())

					IF (@dst_type = 'spring forward')
					BEGIN
						SET @active_start_time_adjusted = (SELECT CAST(DATEADD(HOUR, -1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_start_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_start_time_adjusted_int = CAST(REPLACE(@active_start_time_adjusted, ':', '') AS DECIMAL)

						SET @active_end_time_adjusted = (SELECT CAST(DATEADD(HOUR, -1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_end_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_end_time_adjusted_int = CAST(REPLACE(@active_end_time_adjusted, ':', '') AS DECIMAL)

						IF (@active_start_time = 0)
						BEGIN
							SET @active_start_time_adjusted_int = 230000
						END
						IF (@active_end_time = 5959)
						BEGIN
							SET @active_end_time_adjusted_int = 235959
						END
					END

					IF (@dst_type = 'fall back')
					BEGIN
						SET @active_start_time_adjusted = (SELECT CAST(DATEADD(HOUR, 1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_start_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_start_time_adjusted_int = CAST(REPLACE(@active_start_time_adjusted, ':', '') AS DECIMAL)

						SET @active_end_time_adjusted = (SELECT CAST(DATEADD(HOUR, 1, STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(@active_end_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')) AS TIME(0)))
						SET @active_end_time_adjusted_int = CAST(REPLACE(@active_end_time_adjusted, ':', '') AS DECIMAL)

						IF (@active_start_time = 0)
						BEGIN					
							SET @active_start_time_adjusted_int = 10000
						END

						IF (@active_end_time = 235959)
						BEGIN
							SET @active_end_time_adjusted_int = 5959
						END
					END

					IF (@ss_freq_subday_type IN (0,1))
					BEGIN
						UPDATE #dst_debug
						SET 
							active_start_time_adjusted = @active_start_time_adjusted_int
						WHERE
							id = @scope_identity
					END

					IF (@ss_freq_subday_type NOT IN (0,1))
					BEGIN
						UPDATE #dst_debug
						SET 
							active_start_time_adjusted = @active_start_time_adjusted_int,
							active_end_time_adjusted = @active_end_time_adjusted_int
						WHERE
							id = @scope_identity
					END
								
				FETCH NEXT FROM cur_dst INTO @job_name, @job_id, @schedule_id, @active_start_time, @active_end_time, @ss_freq_subday_type
				END
			CLOSE cur_dst
			DEALLOCATE cur_dst

			SELECT * FROM #dst_debug ORDER BY job_name ASC
		END

	END
END
stopExec:
	IF (@err_msg) IS NOT NULL
		SELECT @err_msg

