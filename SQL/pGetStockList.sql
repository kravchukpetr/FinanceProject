USE [Finance]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pGetStockList]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[pGetStockList]
GO
create procedure pGetStockList
@Market varchar(10) =  NULL,
@IsLoad int = 1
as
begin
	IF @Market IS NOT NULL
	BEGIN
		select * from Stock WHERE Market = @Market AND IsLoad = @IsLoad
	END
	ELSE
	BEGIN
		select * from Stock WHERE IsLoad = @IsLoad
	END

end
