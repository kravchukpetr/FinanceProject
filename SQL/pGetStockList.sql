drop function if exists finance.pGetStockList;
CREATE function finance.pGetStockList(
v_Market varchar(10) =  NULL,
v_IsLoad int = 1
)
returns TABLE ( Stock varchar(10),
				Security varchar(300),
				Sector varchar(100),	
				SubIndustry  varchar(100),
				Market varchar(50),
				LoadDt timestamp,
				IsLoad int)
language plpgsql
as $$
begin
	IF v_Market IS NOT NULL THEN
		return query 	
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Market, s.LoadDt, s.IsLoad 
		FROM finance.Stock s
		WHERE Market = v_Market
		AND s.IsLoad = v_IsLoad;
	ELSE
		return query 
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Market, s.LoadDt, s.IsLoad 
		FROM finance.Stock s
		WHERE s.IsLoad = v_IsLoad;
	END IF;
end; $$
