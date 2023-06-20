drop procedure if exists finance.p_load_recomendation;
CREATE procedure finance.p_load_recomendation(
v_stock varchar(10),
v_period varchar(10),
v_recomendation varchar(10),
v_buy_count int,
v_sell_count int,
v_neutral_count int
)
language plpgsql
as $$
begin

INSERT INTO finance.recomendation(
    stock,
    period,
    recomendation,
    buy_count,
    sell_count,
    neutral_count
)
	SELECT  v_stock,
            v_period,
            v_recomendation,
            v_buy_count,
            v_sell_count,
            v_neutral_count;

end; $$
