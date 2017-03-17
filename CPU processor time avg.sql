declare @ServerName varchar(50) = 'dbase'
declare @CounterSet varchar(50) = 'processor'
declare @Counter varchar(50) = '% processor time'


set @CounterSet = '%' + @CounterSet + '%'
set @Counter = '%' + @Counter + '%'

select 
	ServerName, DateTimeStamp,
	avg(CounterValue) as AverageCounterValue
from
	dbo.PerfmonImport
where
	ServerName = @ServerName
	and CounterInstance like @CounterSet
	and CounterInstance like @Counter
	and CounterInstance not like '%Process(sqlservr)%'
	and CounterInstance not like '%_Total%'
group by
	ServerName, DateTimeStamp
order by 
	DateTimeStamp