drop view if exists finance.v_get_rec;
create view finance.v_get_rec
as
with
last_rec as
(
select stock, period, max(loaddt) loaddt
from finance.recomendation
group by stock, period
)
, rec as
(
select r.*
from finance.recomendation r
join last_rec lr on r.stock = lr.stock and r.period = lr.period and r.loaddt = lr.loaddt
)
, rec_1d as (
	select *
	from rec
	where period = '1d'
)
,
rec_1w as (
	select *
	from rec
	where period = '1W'
)
,
rec_1m as (
	select *
	from rec
	where period = '1M'
)
,
result_rec as (
select rec_1d.stock,
	   s."security",
	   rec_1d.recomendation as rec_1d,
	   rec_1w.recomendation as rec_1w,
	   rec_1m.recomendation as rec_1m,
   	   rec_1d.buy_count as buy_count_1d,
	   rec_1d.sell_count as sell_count_1d,
	   rec_1d.neutral_count as neutral_count_1d,
   	   rec_1w.buy_count as buy_count_1w,
	   rec_1w.sell_count as sell_count_1w,
	   rec_1w.neutral_count as neutral_count_1w,
	   rec_1m.buy_count as buy_count_1m,
	   rec_1m.sell_count as sell_count_1m,
	   rec_1m.neutral_count as neutral_count_1m
from rec_1d
left join rec_1w on rec_1d.stock = rec_1w.stock
left join rec_1m on rec_1d.stock = rec_1m.stock
left join finance.stock s on rec_1d.stock = s.stock
)
select *
from result_rec