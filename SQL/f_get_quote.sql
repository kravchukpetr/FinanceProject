drop function if exists finance.f_get_quote;
create function finance.f_get_quote(
	v_dt_from date,
	v_dt_to date,
	v_stock varchar(8000)
)
returns table (
Dt date,
Stock varchar(10),
OpenValue numeric(20,6),
HighValue numeric(20,6),
LowValue  numeric(20,6),
CloseValue numeric(20,6),
AdjClose numeric(20,6),
Volume numeric(20,6)
)

language plpgsql
as $$
begin
	IF v_stock IS NOT null then
		return query
		SELECT q.dt, q.stock, q.openvalue, q.highvalue , q.lowvalue , q.closevalue , q.adjclose , q.volume
		FROM finance.Quotes q
		JOIN (SELECT UNNEST(STRING_TO_ARRAY(v_stock, ',')) Stock) s ON q.Stock = s.Stock
		WHERE (q.dt between v_dt_from AND v_dt_to OR (v_dt_from IS NULL AND v_dt_to IS NULL));
	else
		return query
		SELECT q.dt, q.stock, q.openvalue, q.highvalue , q.lowvalue , q.closevalue , q.adjclose , q.volume
		FROM Quotes q
		WHERE q.dt between v_dt_from AND v_dt_to;
	end if;
end; $$
