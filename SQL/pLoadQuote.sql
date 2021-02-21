USE [Finance]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pLoadQuote]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[pLoadQuote]
GO
create procedure pLoadQuote
@Dt date,
@Stock varchar(10),
@OpenValue numeric(20,6),	
@HighValue numeric(20,6),	
@LowValue  numeric(20,6),
@CloseValue numeric(20,6),	
@AdjClose numeric(20,6),	
@Volume numeric(20,6)
as
begin
IF EXISTS(SELECT 1 FROM Quotes WHERE Dt = @Dt AND Stock = @Stock)
BEGIN
	DELETE FROM Quotes WHERE Dt = @Dt AND Stock = @Stock	
END

INSERT INTO Quotes
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
	SELECT @Dt,
	@Stock,
	@OpenValue,	
	@HighValue,	
	@LowValue,
	@CloseValue,	
	@AdjClose,	
	@Volume



end