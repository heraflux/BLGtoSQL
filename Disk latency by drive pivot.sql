declare @ServerName varchar(50) = 'dbase'
declare @CounterSet varchar(50) = 'PhysicalDisk'
declare @Counter varchar(50) = 'Avg. Disk sec/Read'


set @CounterSet = '%' + @CounterSet + '%'
set @Counter = '%' + @Counter + '%'

declare @cols nvarchar(max), @cols2 nvarchar(max), @sql nvarchar(max);
set @cols = N'';
set @cols2 = N'';


select 
	ServerName, DateTimeStamp,
	SUBSTRING(CounterInstance, LEN(LEFT(CounterInstance, CHARINDEX ('(', CounterInstance))) + 1, 
		LEN(CounterInstance) - LEN(LEFT(CounterInstance, CHARINDEX ('(', CounterInstance))) - 
		LEN(RIGHT(CounterInstance, LEN(CounterInstance) - CHARINDEX (')', CounterInstance))) - 1) as DiskDrive,
	CounterValue * 1000. as CounterValue
into #tmpPerfData
from
	dbo.PerfmonImport
where
	ServerName = @ServerName
	and CounterInstance like @CounterSet
	and CounterInstance like @Counter
	and CounterInstance not like '%Process(sqlservr)%'
	and CounterInstance not like '%_Total%'



select @cols += N'], [' + DiskDrive from
	(select top 100 percent 
		DiskDrive
	from 
		(select distinct DiskDrive from #tmpPerfData) 
	as Y order by DiskDrive) as X order by DiskDrive;

select @cols = stuff(@cols, 1, 2, '') + ']'

select @cols2 += N', cast(avg([' + DiskDrive + ']) as numeric(5,2)) as [VOL ' + DiskDrive + ']' from 
	(select top 100 percent 
		DiskDrive
	from 
		(select distinct DiskDrive from #tmpPerfData) 
	as Y order by DiskDrive) as X order by DiskDrive;
set @cols2 = stuff(@cols2, 1, 2, '')

set @sql = 'select DateTimeStamp, ' + @cols2 + ' 
from #tmpPerfData 
pivot ( avg(CounterValue) for DiskDrive in (' + @cols + ')) as pvttable
group by DateTimeStamp
order by DateTimeStamp'

print @sql

exec sp_executesql @sql

drop table #tmpPerfData