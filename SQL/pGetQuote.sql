USE [Finance]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pGetQuote]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[pGetQuote]
GO
create procedure pGetQuote
@DtFrom date,
@DtTo date,
@Stock varchar(8000)
as
begin
set nocount on;

create table #stock(StockStr varchar(8000))
create table #stock_list(Stock varchar(10))

	IF @Stock IS NOT NULL
	BEGIN
		
		insert into  #stock(StockStr) select @Stock
		insert into #stock_list(Stock) 
		SELECT LTRIM(value) Stock 
		FROM #stock  
		CROSS APPLY STRING_SPLIT(StockStr, ',')

		select q.* 
		from Quotes q 
		join #stock_list s ON q.Stock = s.Stock
		WHERE (Dt between @DtFrom AND @DtTo OR (@DtFrom IS NULL AND @DtTo IS NULL))
	END
	ELSE
	BEGIN
		select * from Quotes WHERE Dt between @DtFrom AND @DtTo
	END

end