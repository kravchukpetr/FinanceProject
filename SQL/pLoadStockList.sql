USE [Finance]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pLoadStockList]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].pLoadStockList
GO
create procedure pLoadStockList
@Stock varchar(10),
@Security varchar(300),
@Sector varchar(100),
@SubIndustry varchar(100)
as
begin

IF NOT EXISTS (SELECT 1 FROM Stock WHERE Stock = @Stock)
BEGIN

	INSERT INTO Stock
	(
	Stock,
	Security,
	Sector,	
	SubIndustry 
	)
	SELECT @Stock,
		   @Security,
		   @Sector,
		   @SubIndustry
	END

end