select count(*)
from finance.forex;

select screener, count(*)
from finance.stock
group by screener;


select stock, count(*)
from finance.forex
group by stock;

select count(*)
from finance.quotes;

select stock, count(*)
from finance.quotes
group by stock;

select count(*)
from (
    select stock, count(*)
    from finance.quotes
    group by stock
     ) res;


select * from finance.stock;

select is_load, count(*)
from finance.stock
group by is_load;


update finance.stock
set is_load = 1
where stock not in (
    select distinct stock
    from finance.quotes
);

select is_load, count(*)
from finance.f_get_stock_list(null)
group by is_load;

select *
from finance.v_get_rec
where stock IN ('EURSEK', 'AUDCHF', 'NZDUSD');

select max(loaddt)
from finance.recomendation
limit 100;

select *
from finance.v_get_rec
where screener = 'Forex' AND rec_1d = 'STRONG_BUY'

select *
from finance.v_get_rec
where screener = 'Forex' AND rec_1d = 'STRONG_SELL'


select *
from finance.v_get_rec
where screener = 'america' AND rec_1d = 'STRONG_BUY' AND rec_1w = 'STRONG_BUY'

select *
from finance.v_get_rec
where screener = 'Forex' AND rec_1d = 'STRONG_SELL'

select stock, count(*)
from finance.f_get_quote ('2022-07-06', '2023-07-05', NULL, 'america')
group by stock

select count(*)
from finance.quotes
-- where stock = 'AAPL'
-- order by dt desc;

select is_load, count(*)
from finance.stock
group by is_load

select count(*)
from finance.stock
where stock = 'PACB'
limit 100;

select count(*)
from finance.quotes
where stock = 'PACB'
limit 100;

select count(*)
from finance.quotes;

select stock, count(*)
from finance.quotes
group by stock;


select count(*)
from finance.forex;

select stock, count(*)
from finance.forex
group by stock;

select stock, count(*), min(dt) min_dt, max(dt) max_dt
from finance.forex
where stock = 'USDCHF'
group by stock;

select *
from finance.forex
where stock = 'USDCHF'
limit 100;

select max(loaddt)
from finance.recomendation;

select *
from finance.recomendation
limit 100;

select *
from finance.stock
limit 10;

select is_load, count(*)
from finance.stock
group by is_load;

select screener, exchange,  count(*) cnt
from finance.stock
group by screener, exchange;


select count(*)
from finance.f_get_stock_list('america')

select count(*)
from finance.Stock;

SELECT VERSION()

select count(*)
from finance.quotes;

select stock, count(*)
from finance.quotes
group by stock
order by stock;

select count(*)
from (select stock, count(*)
      from finance.quotes
      group by stock) r;

delete from finance.quotes;


select *
from finance.recomendation r
join finance.stock s on r.stock = s.stock
where s.screener <> 'Forex'
limit 100;

with last_rec_of_day as
         (select stock,  cast(loaddt as date) + INTERVAL '1 day' as dt, max(loaddt) loaddt
          from finance.recomendation
          where
--             stock = 'AAPL' and
            period = '1d'
            and cast(loaddt as date) <> cast(now() as date)
          group by stock, cast(loaddt as date) + INTERVAL '1 day'
          )
, rec as (select l.dt, r.*
          from finance.recomendation r
          join last_rec_of_day l on r.stock = l.stock and r.loaddt = l.loaddt
          join finance.stock s on r.stock = s.stock
          where s.screener <> 'Forex' AND r.period = '1d'
        )
, trade as (
    select stock, dt, adjclose, lag(adjclose, -1) over (partition by  stock order by dt desc) prev_adjclose
    from finance.quotes
--     where stock = 'AAPL'
)
, trade_result as (select t.stock,
                          t.dt,
                          t.adjclose,
                          t.prev_adjclose,
                          round(((t.adjclose - t.prev_adjclose) / t.prev_adjclose) * 100, 2) pct_change,
                          r.recomendation,
                          r.buy_count,
                          r.sell_count,
                          r.neutral_count
                   from trade t
                   join rec r on t.stock = r.stock and t.dt = r.dt
                   order by dt desc)
, stats as (select case
                       when tr.pct_change > 0 then 'BUY'
                       when tr.pct_change = 0 then 'NEUTRAL'
                       else 'SELL'
                       end as result,
                   tr.buy_count - tr.sell_count - tr.neutral_count as coef_buy,
                   tr.*
            from trade_result tr)

select result, count(*)
from stats
where recomendation = 'STRONG_BUY'
  --and dt = '2023-07-07'
group by result
-- order by coef_buy desc;



-- select sum(case when result = recomendation then 1 else 0 end) as total_win, count(*) cnt
-- select recomendation, count(*)
-- from stats
-- where /*recomendation = 'BUY' and*/ dt = '2023-07-07'
-- group by recomendation
-- order by dt, coef_buy desc


select *
from finance.recomendation




