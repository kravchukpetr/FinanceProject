drop function if exists finance.f_get_stock_list;
CREATE function finance.f_get_stock_list(
v_Market varchar(10) =  NULL,
v_IsLoad int = 1
)
returns TABLE ( Stock varchar(10),
				Security varchar(300),
				Sector varchar(100),	
				SubIndustry  varchar(100),
				Exchange varchar(50),
				LoadDt timestamp,
				IsLoad int,
				Screener varchar(50))
language plpgsql
as $$
begin
	IF v_Market IS NOT NULL THEN
		return query 	
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Exchange, s.LoadDt, s.IsLoad, s.Screener
		FROM finance.Stock s
		WHERE Market = v_Market
		AND s.is_load = v_IsLoad;
	ELSE
		return query 
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Exchange, s.LoadDt, s.IsLoad, s.Screener
		FROM finance.Stock s
		WHERE s.is_load = v_IsLoad;
	END IF;
end; $$
