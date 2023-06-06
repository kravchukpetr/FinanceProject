drop procedure if exists finance.pLoadQuote;
CREATE procedure finance.pLoadQuote(
v_Dt date,
v_Stock varchar(10),
v_OpenValue numeric(20,6),	
v_HighValue numeric(20,6),	
v_LowValue  numeric(20,6),
v_CloseValue numeric(20,6),	
v_AdjClose numeric(20,6),	
v_Volume numeric(20,6)
)
language plpgsql
as $$
begin
IF EXISTS(SELECT 1 FROM finance.Quotes WHERE Dt = v_Dt AND Stock = v_Stock) THEN
	DELETE FROM finance.Quotes WHERE Dt = v_Dt AND Stock = v_Stock;
END IF;

INSERT INTO finance.Quotes
	(
	Dt,
	Stock,
	OpenValue,	
	HighValue,	
	LowValue,
	CloseValue,	
	AdjClose,	
	Volume
	)
	SELECT v_Dt,
	v_Stock,
	v_OpenValue,	
	v_HighValue,	
	v_LowValue,
	v_CloseValue,	
	v_AdjClose,	
	v_Volume;

end; $$