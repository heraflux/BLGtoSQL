declare @ServerName varchar(50) = 'Chewy'
declare @CounterSet varchar(50) = 'processor'
declare @Counter varchar(50) = '% processor time'


set @CounterSet = '%' + @CounterSet + '%'
set @Counter = '%' + @Counter + '%'

declare @cols nvarchar(max), @cols2 nvarchar(max), @sql nvarchar(max);
set @cols = N'';
set @cols2 = N'';


select 
	ServerName, DateTimeStamp,
	CounterInstance as CPUNum, CounterValue
into #tmpPerfData
from
	dbo.PerfmonImport
where
	ServerName = @ServerName
	and CounterSet like @CounterSet
	and CounterName like @Counter
	and CounterSet not like '%Process(sqlservr)%'
	and CounterInstance not like '%_Total%'
	and DateTimeStamp between '2019-02-06 00:00:00' and '2019-02-07 00:00:00'



select @cols += N'], [' + CPUNum from
	(select top 100 percent 
		CPUNum
	from 
		(select distinct case when len(cast(CPUNum as varchar)) = 1 then 
		'0' + cast(CPUNum as varchar) else cast(CPUNum as varchar) end as CPUNum from #tmpPerfData) 
	as Y order by CPUNum) as X order by CPUNum;

select @cols = stuff(@cols, 1, 2, '') + ']'

select @cols2 += N', cast(avg([' + CPUNum + ']) as numeric(5,2)) as [CPU' + CPUNum + ']' from 
	(select top 100 percent 
		CPUNum
	from 
		(select distinct case when len(cast(CPUNum as varchar)) = 1 then 
		'0' + cast(CPUNum as varchar) else cast(CPUNum as varchar) end as CPUNum from #tmpPerfData) 
	as Y order by CPUNum) as X order by CPUNum;
set @cols2 = stuff(@cols2, 1, 2, '')

set @sql = 'select DateTimeStamp, ' + replace(@cols2,'[0','[') + ' 
from #tmpPerfData 
pivot ( avg(CounterValue) for cpunum in (' + replace(@cols,'[0','[') + ')) as pvttable
group by DateTimeStamp
order by DateTimeStamp'

print @sql

exec sp_executesql @sql

drop table #tmpPerfData
