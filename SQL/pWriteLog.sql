USE [Finance]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pWriteLog]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].pWriteLog
GO
create procedure pWriteLog
@TypeWrite int, 
@WF_ID int, 
@WF_STATUS int, 
@CNT_ERRORS int = 0,
@LogTxt varchar(8000) = NULL
as
begin

DECLARE @ID bigint

IF @TypeWrite = 1
BEGIN
	INSERT INTO WorkFlowLogs
	(
	WF_ID, 
	WF_STATUS 
	)
	SELECT @WF_ID, @WF_STATUS
END

IF @TypeWrite = 2
BEGIN
	
	SELECT @ID = MAX(ID) FROM WorkFlowLogs WHERE WF_ID = @WF_ID

	Update WorkFlowLogs
	SET WF_STATUS = @WF_STATUS, 
		CNT_ERRORS  = @CNT_ERRORS,
		EndDt = getdate(),
		LOG_TXT = @LogTxt
	WHERE ID = @ID
END


end