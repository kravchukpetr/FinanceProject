drop procedure if exists wf.pWriteLog;
CREATE procedure wf.pWriteLog(
v_TypeWrite int, 
v_WF_ID int, 
v_WF_STATUS int, 
v_CNT_ERRORS int = 0,
v_LogTxt varchar(8000) = NULL
)
language plpgsql
as $$
DECLARE v_ID bigint;
begin

IF v_TypeWrite = 1 THEN
	INSERT INTO wf.WorkFlowLogs
	(
	WF_ID, 
	WF_STATUS 
	)
	SELECT v_WF_ID, v_WF_STATUS;
END IF;

IF v_TypeWrite = 2 THEN
	
	SELECT v_ID = MAX(ID) FROM WorkFlowLogs WHERE WF_ID = v_WF_ID;

	Update wf.WorkFlowLogs
	SET WF_STATUS = v_WF_STATUS, 
		CNT_ERRORS  = v_CNT_ERRORS,
		EndDt = now(),
		LOG_TXT = v_LogTxt
	WHERE ID = v_ID;
END IF;

end; $$;
