create schema if not exists finance;

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


