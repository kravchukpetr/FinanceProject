select * 
from finance.recomendation r 
limit 100

select count(*), count(*)/3
from finance.recomendation

select * 
from finance.stock s;

select exchange, screener, count(*) 
from finance.stock s
group by exchange, screener;


select * 
from finance.quotes s
limit 100;


select * 
from finance.v_get_rec
where rec_1d = 'STRONG_BUY' and rec_1w = 'STRONG_BUY' and  screener = 'america';

select * 
from finance.v_get_rec
where rec_1d = 'STRONG_SELL' and rec_1w = 'STRONG_SELL' and  screener = 'america';

select * 
from finance.v_get_rec
where rec_1d = 'STRONG_BUY' /*and rec_1w = 'STRONG_BUY'*/ and  screener = 'Forex'

select * 
from finance.v_get_rec
where rec_1d = 'STRONG_SELL' /*and rec_1w = 'STRONG_SELL'*/ and  screener = 'Forex'



select * from finance.f_get_quote (NULL, NULL, 'AAPL', 'america')
select * from finance.f_get_quote (NULL, NULL, 'EURUSD', 'Forex')

 select screener, count(*)
 from finance.stock
 group by screener

 
 SELECT name, setting FROM pg_settings WHERE category = 'File Locations';


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



drop table if exists finance.forex;
create table if not exists  finance.forex
(
dt timestamp,
Stock varchar(10),
OpenValue numeric(20,6),	
HighValue numeric(20,6),	
LowValue  numeric(20,6),
CloseValue numeric(20,6),	
AdjClose numeric(20,6),	
Volume numeric(20,6),
LoadDt timestamp default now(),
primary key (Dt, Stock)
);


CALL finance.p_load_quote('2023-06-28 00:00:00', 'EURUSD', 'Forex', 1.0960105657577515, 1.0962508916854858, 1.0957703590393066, 1.0961307287216187, 1.0961307287216187, 0)

select *
from finance.forex;

select *
from finance.quotes q 
where stock = 'BEN'

select max(loaddt)
from finance.quotes q 


select stock, count(*) cnt, min(dt) min_dt, max(dt) min_dt
from finance.quotes q 
group by stock

select * 
from finance.forex f 
limit 100;

select stock, count(*) cnt, min(dt) min_dt, max(dt) min_dt 
from finance.forex f 
group by stock;

select max(loaddt)
from finance.recomendation r;


select stock, count(*) cnt, min(loaddt) min_dt, max(loaddt) min_dt 
from finance.recomendation r 
group by stock;



select stock, count(*)
from finance.quotes q
group by stock;

select *
from wf.workflowlogs w 

with max_dt as (
	select stock, max(dt) dt
	from finance.quotes q
	group by stock
	union all
	select stock, max(dt) dt
	from finance.forex f
	group by stock
)
, actual_price as (
	select q.stock, closevalue 
	from finance.quotes q
	join max_dt on q.stock = max_dt.stock and q.dt = max_dt.dt
	union all 
	select q.stock, closevalue 
	from finance.forex q
	join max_dt on q.stock = max_dt.stock and q.dt = max_dt.dt

)
select count(*)
from actual_price
