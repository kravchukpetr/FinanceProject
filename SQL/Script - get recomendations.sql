select * 
from finance.recomendation r 
limit 100

select count(*), count(*)/3
from finance.recomendation

select * 
from finance.stock s 


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
)
select * 
from result_rec




select * 
from finance.v_get_rec
where rec_1d = 'STRONG_BUY' and rec_1w = 'STRONG_BUY' and  screener = 'america';

select * 
from finance.v_get_rec
where rec_1d = 'STRONG_BUY' /*and rec_1w = 'STRONG_BUY'*/ and  screener = 'Forex'

select * 
from finance.v_get_rec
where rec_1d = 'STRONG_SELL' /*and rec_1w = 'STRONG_SELL'*/ and  screener = 'Forex'


select rec_1d, count(*) 
from finance.v_get_rec
group by rec_1d
order by rec_1d desc

select * from  finance.stock where screener = 'Forex'



create table #stock(StockStr varchar(8000));
create table #stock_list(Stock varchar(10));



insert into  #stock(StockStr) select @Stock
insert into #stock_list(Stock) 
SELECT LTRIM(value) Stock 
FROM #stock  
CROSS APPLY STRING_SPLIT('AAPL', ',')


select UNNEST(STRING_TO_ARRAY('AAPL, BSX', ','));

select * from finance.f_get_quote('2023-05-01', '2023-05-15', 'AAPL');

select * from  finance.stock where screener = 'america'		
select * from finance.f_get_stock_list(NULL);		
select * from finance.f_get_stock_list('Forex');

"screener" 

