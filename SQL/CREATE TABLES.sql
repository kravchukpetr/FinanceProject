use Finance
go
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Quotes]') AND type in (N'U', N'V'))
DROP TABLE [dbo].[Quotes]
go
create table Quotes
(
Dt date,
Stock varchar(10),
OpenValue numeric(20,6),	
HighValue numeric(20,6),	
LowValue  numeric(20,6),
CloseValue numeric(20,6),	
AdjClose numeric(20,6),	
Volume numeric(20,6),
LoadDt datetime default getdate(),
primary key (Dt, Stock)
)

go
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Stock]') AND type in (N'U', N'V'))
DROP TABLE [dbo].[Stock]
go
create table Stock
(
Stock varchar(10),
Security varchar(300),
Sector varchar(100),	
SubIndustry  varchar(100),
Market varchar(50),
LoadDt datetime default getdate(),
primary key (Stock)
)
go

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[WorkFlowLogs]') AND type in (N'U', N'V'))
DROP TABLE [dbo].[WorkFlowLogs]
go
create table WorkFlowLogs
(
ID bigint IDENTITY(1,1),
WF_ID int, 
WF_STATUS int, 
CNT_ERRORS int,
LoadDt datetime default getdate(),
EndDt datetime,
LOG_TXT VARCHAR(8000),
primary key (ID)
)
go

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[WorkFlows]') AND type in (N'U', N'V'))
DROP TABLE [dbo].[WorkFlows]
go
create table WorkFlows
(
WF_ID int IDENTITY(1,1), 
WF_SC VARCHAR(100), 
WF_NM VARCHAR(500), 
primary key (WF_ID)
)
go

INSERT INTO  WorkFlows (WF_SC, WF_NM) SELECT 'FinanceDailyLoad', '≈жедневна€ загрузка данных по котировкам'


