select * 
from finance.recomendation r 
limit 100

select count(*)/3
from finance.recomendation



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
join last_rec lr on r.stock = lr.stock and l.period = lr.period and r.loaddt = lr.loaddt
)
, rec_1d as (
	select * 
	from rec 
	period = '1d'
)
, 
rec_1w as (
	select * 
	from rec 
	period = '1W'
)
, 
rec_1m as (
	select * 
	from rec 
	period = '1M'
)
select rec_1d.recomendation as 1d_rec, rec_1w.recomendation as 1w_rec, rec_1m.recomendation as 1m_rec
from rec_1d
left join rec_1w on rec_1d.stock = rec_1w.stock
left join rec_1m on rec_1d.stock = rec_1m.stock
