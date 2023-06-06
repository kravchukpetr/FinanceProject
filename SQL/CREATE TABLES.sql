create schema if not exists finance;

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
Market varchar(50),
LoadDt timestamp default now(),
IsLoad int default,
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

INSERT INTO  wf.WorkFlows (WF_SC, WF_NM) SELECT 'FinanceDailyLoad', '≈жедневна€ загрузка данных по котировкам';


