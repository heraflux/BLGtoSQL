declare @ServerName varchar(50) = 'dbase'
declare @CounterSet varchar(50) = 'physicaldisk'
declare @Counter varchar(50) = 'Avg. Disk sec/Read'


set @CounterSet = '%' + @CounterSet + '%'
set @Counter = '%' + @Counter + '%'

select 
	ServerName, DateTimeStamp,
	SUBSTRING(CounterInstance, LEN(LEFT(CounterInstance, CHARINDEX ('(', CounterInstance))) + 1, 
		LEN(CounterInstance) - LEN(LEFT(CounterInstance, CHARINDEX ('(', CounterInstance))) - 
		LEN(RIGHT(CounterInstance, LEN(CounterInstance) - CHARINDEX (')', CounterInstance))) - 1) as DiskDrive,
	CounterValue * 1000. as CounterValue
from
	dbo.PerfmonImport
where
	ServerName = @ServerName
	and CounterInstance like @CounterSet
	and CounterInstance like @Counter
	and CounterInstance not like '%Process(sqlservr)%'
	and CounterInstance not like '%_Total%'
order by 
	DateTimeStamp, DiskDrive