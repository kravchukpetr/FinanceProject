drop procedure if exists finance.p_load_stock_list;
CREATE procedure finance.p_load_stock_list(
v_Stock varchar(10),
v_Security varchar(300),
v_Sector varchar(100),
v_SubIndustry varchar(100)
)
language plpgsql
as $$
begin
	if NOT EXISTS (SELECT 1 FROM finance.Stock WHERE Stock = v_Stock) then 
		INSERT INTO finance.Stock
		(
		Stock,
		Security,
		Sector,	
		SubIndustry 
		)
		SELECT v_Stock,
			   v_Security,
			   v_Sector,
			   v_SubIndustry;
	end if;
end; $$
