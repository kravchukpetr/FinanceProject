create schema if not exists finance;
drop view if exists finance.v_get_rec;

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

drop table if exists finance.Quotes;
create table if not exists  finance.Quotes
(
Dt date,
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

drop table if exists finance.Stock;
create table if not exists  finance.Stock
(
Stock varchar(10),
Security varchar(300),
Sector varchar(100),
SubIndustry  varchar(100),
Exchange varchar(50),
Screener varchar(30),
LoadDt timestamp default now(),
is_load int default 1,
is_avaible_for_trade int default 1,
primary key (Stock)
);

create schema if not exists wf;

drop table if exists wf.Stock;
create table if not exists wf.WorkFlowLogs
(
ID serial,
WF_ID int,
WF_STATUS int,
CNT_ERRORS int,
LoadDt timestamp default now(),
EndDt timestamp,
LOG_TXT VARCHAR(8000),
primary key (ID)
);

drop table if exists wf.WorkFlows;
create table  if not exists wf.WorkFlows
(
WF_ID serial,
WF_SC VARCHAR(100),
WF_NM VARCHAR(500),
primary key (WF_ID)
);

INSERT INTO  wf.WorkFlows (WF_SC, WF_NM) SELECT 'FinanceDailyLoad', 'Load Finance quotes';


drop table if exists finance.recomendation;
create table finance.recomendation(
loaddt timestamp default now(),
stock varchar(100),
period varchar(10),
recomendation varchar(50),
buy_count int,
sell_count int,
neutral_count int
);

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
end; $$;
drop function if exists finance.f_get_stock_list;
CREATE function finance.f_get_stock_list(
v_screener varchar(10) =  NULL,
v_IsLoad int = 1
)
returns TABLE ( Stock varchar(10),
				Security varchar(300),
				Sector varchar(100),
				SubIndustry  varchar(100),
				Exchange varchar(50),
				LoadDt timestamp,
				is_Load int,
				Screener varchar(50),
				is_avaible_for_trade int
				)
language plpgsql
as $$
begin
	IF v_screener IS NOT NULL THEN
		return query
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Exchange, s.LoadDt, s.is_Load, s.Screener, s.is_avaible_for_trade
		FROM finance.Stock s
		WHERE s.screener = v_screener AND s.is_load = v_IsLoad;
	ELSE
		return query
		SELECT s.Stock, s.Security, s.Sector, s.SubIndustry, s.Exchange, s.LoadDt, s.is_Load, s.Screener, s.is_avaible_for_trade
		FROM finance.Stock s
		WHERE s.is_load = v_IsLoad;
	END IF;
end; $$;
drop procedure if exists finance.p_load_quote;
CREATE procedure finance.p_load_quote(
v_Dt date,
v_Stock varchar(10),
v_OpenValue numeric(20,6),
v_HighValue numeric(20,6),
v_LowValue  numeric(20,6),
v_CloseValue numeric(20,6),
v_AdjClose numeric(20,6),
v_Volume numeric(20,6)
)
language plpgsql
as $$
begin
	IF EXISTS(SELECT 1 FROM finance.Quotes WHERE Dt = v_Dt AND Stock = v_Stock) THEN
		DELETE FROM finance.Quotes WHERE Dt = v_Dt AND Stock = v_Stock;
	END IF;

	INSERT INTO finance.Quotes
		(
		Dt,
		Stock,
		OpenValue,
		HighValue,
		LowValue,
		CloseValue,
		AdjClose,
		Volume
		)
		SELECT v_Dt,
		v_Stock,
		v_OpenValue,
		v_HighValue,
		v_LowValue,
		v_CloseValue,
		v_AdjClose,
		v_Volume;

end; $$;


drop procedure if exists finance.p_load_forex;
CREATE procedure finance.p_load_forex(
v_Dt timestamp,
v_Stock varchar(10),
v_OpenValue numeric(20,6),
v_HighValue numeric(20,6),
v_LowValue  numeric(20,6),
v_CloseValue numeric(20,6),
v_AdjClose numeric(20,6),
v_Volume numeric(20,6)
)
language plpgsql
as $$
begin
	IF EXISTS(SELECT 1 FROM finance.forex WHERE Dt = v_Dt AND Stock = v_Stock) THEN
		DELETE FROM finance.forex WHERE Dt = v_Dt AND Stock = v_Stock;
	END IF;

	INSERT INTO finance.forex
		(
		Dt,
		Stock,
		OpenValue,
		HighValue,
		LowValue,
		CloseValue,
		AdjClose,
		Volume
		)
		SELECT v_Dt,
		v_Stock,
		v_OpenValue,
		v_HighValue,
		v_LowValue,
		v_CloseValue,
		v_AdjClose,
		v_Volume;

end; $$;

drop procedure if exists finance.p_load_recomendation;
CREATE procedure finance.p_load_recomendation(
v_stock varchar(10),
v_period varchar(10),
v_recomendation varchar(50),
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

end; $$;

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
end; $$;

drop procedure if exists wf.pWriteLog;
CREATE procedure wf.pWriteLog(
v_TypeWrite int,
v_WF_ID int,
v_WF_STATUS int,
v_CNT_ERRORS int = 0,
v_LogTxt varchar(8000) = NULL
)
language plpgsql
as $$
DECLARE v_ID bigint;
begin

IF v_TypeWrite = 1 THEN
	INSERT INTO wf.WorkFlowLogs
	(
	WF_ID,
	WF_STATUS
	)
	SELECT v_WF_ID, v_WF_STATUS;
END IF;

IF v_TypeWrite = 2 THEN

	SELECT v_ID = MAX(ID) FROM WorkFlowLogs WHERE WF_ID = v_WF_ID;

	Update wf.WorkFlowLogs
	SET WF_STATUS = v_WF_STATUS,
		CNT_ERRORS  = v_CNT_ERRORS,
		EndDt = now(),
		LOG_TXT = v_LogTxt
	WHERE ID = v_ID;
END IF;

end; $$;

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
       s.exchange,
	   s.screener,
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
join finance.stock s on rec_1d.stock = s.stock
left join rec_1w on rec_1d.stock = rec_1w.stock
left join rec_1m on rec_1d.stock = rec_1m.stock
where s.is_avaible_for_trade = 1
)
select *
from result_rec;
