declare @ServerName varchar(50) = 'dbase'
declare @CounterSet varchar(50) = 'processor'
declare @Counter varchar(50) = '% processor time'


set @CounterSet = '%' + @CounterSet + '%'
set @Counter = '%' + @Counter + '%'

select 
	ServerName, DateTimeStamp,
	cast(SUBSTRING(CounterSet, LEN(LEFT(CounterSet, CHARINDEX ('(', CounterSet))) + 1, 
		LEN(CounterSet) - LEN(LEFT(CounterSet, CHARINDEX ('(', CounterSet))) - 
		LEN(RIGHT(CounterSet, LEN(CounterSet) - CHARINDEX (')', CounterSet))) - 1) as int) as CPUCore,
	CounterValue
from
	dbo.PerfmonImport
where
	ServerName = @ServerName
	and CounterInstance like @CounterSet
	and CounterInstance like @Counter
	and CounterInstance not like '%Process(sqlservr)%'
	and CounterInstance not like '%_Total%'
order by 
	DateTimeStamp, CPUCore
