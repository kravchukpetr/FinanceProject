exec pLoadQuote '2018-12-31 00:00:00', 'AAPL' , 39.63249969482422, 39.84000015258789, 39.119998931884766, 39.435001373291016, 38.518611907958984, 140014000

select * from Quotes order by Dt desc

select count(*) from finance.Quotes;
select stock, count(*) from finance.Quotes group by stock;

call finance.pLoadStockList('DPZ', 'Domino''s Pizza' , 'Consumer Discretionary', 'Restaurants')


select * from finance.f_get_stock_list('Forex');
select * from finance.f_get_stock_list();

select * 
from finance.Stock
limit 100;

select * 
from finance.Quotes
limit 100;

select * from Stock

select ISLoad, COUNT(*) 
from Stock
group by ISLoad


ALTER TABLE finance.Stock ADD IsLoad int default 1


select * from Stock

with StockExist as
(select Stock, count(*) Cnt from Quotes
group by Stock)
UPDATE Stock
SET ISLoad = 0
from Stock
JOIN StockExist ON Stock.Stock = StockExist.Stock 

UPDATE Stock
SET ISLoad = 1
WHERE ISLoad IS NULL



select DISTINCT Stock from Quotes

select top 100 * from  Quotes

--exec pGetStockList

UPDATE Stock
SET ISLoad = 1
WHERE Stock NOT IN ('BF.B', 'BRK.B')


select Stock, count(*) Cnt from Quotes
group by Stock



UPDATE Stock
SET ISLoad = 0
WHERE Stock NOT IN ('EL', 'ES', 'ETN')

select Stock, MAX(LoadDt)
from Quotes WHERE Stock IN ('EL', 'ES', 'ETN')
group by Stock


select * from WorkFlowLogs

call finance.pLoadStockList('MMM', '3M' , 'Industrials', 'Industrial Conglomerates');

CALL finance.pLoadStockList('MMM', '3M' , 'Industrials', 'Industrial Conglomerates');

select * from finance.stock;
select count(*) from finance.stock;

drop table finance.stock;
commit;


select * from finance.quotes q ;
select count(*) from finance.quotes;

select * from finance.pGetStockList(null, 1);

--select * from finance.Stock
