drop procedure if exists wf.pwritelog;
create or replace procedure wf.pwritelog(IN v_typewrite integer, IN v_wf_id integer, IN v_wf_status integer, IN v_cnt_errors integer DEFAULT 0, IN v_logtxt character varying DEFAULT NULL::character varying)
    language plpgsql
as
$$
DECLARE v_ID bigint;
begin

IF v_TypeWrite = 1 THEN
	INSERT INTO postgres.wf.workflowlogs
	(
	WF_ID,
	WF_STATUS
	)
	SELECT v_WF_ID, v_WF_STATUS;
END IF;

IF v_TypeWrite = 2 THEN

	SELECT MAX(ID)
	INTO v_ID
	FROM postgres.wf.workflowlogs WHERE WF_ID = v_WF_ID;

	Update postgres.wf.workflowlogs
	SET WF_STATUS = v_WF_STATUS,
		CNT_ERRORS  = v_CNT_ERRORS,
		EndDt = now(),
		LOG_TXT = v_LogTxt
	WHERE ID = v_ID;
END IF;

end; $$;

alter procedure wf.pwritelog(integer, integer, integer, integer, varchar) owner to postgres;

