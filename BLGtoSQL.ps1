<# 
    .SYNOPSIS 
    Perfmon BLG to SQL Server Importer - by David Klee, Heraflux Technologies
    http://www.heraflux.com
    
    .DESCRIPTION
        The purpose of this script is to allow DBAs to collect Windows Perfmon data in the common format 
        of BLG files and automatically extract the Perfmon data and load it into a SQL Server database
        for further analysis.
        
        The trouble with conventional CSV file import processes is that Perfmon data can have a variable
        number of columns between servers, or the columns can grow over time as more counters are added.
        The script accommodates this change by fetching the Perfmon data column by column.
    
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        Obligatory Disclaimer
        THE SCRIPT AND PARSER IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE 
        INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY 
        SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA 
        OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION 
        WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    
    Sample usage of this script:
        ./BLGToSQL.ps1 -PerfmonDirectory "E:\PerfmonData" -ServerName="PRDSQL01" 
        
    .LINK
    Fetch this script it its most updated form at:
    https://github.com/heraflux/BLGtoSQL  
    
    .PARAMETER PerfmonDirectory
    The location of the Perfmon BLG file folder.

    .PARAMETER ServerName
    The server name this script is collecting from, so that the target database is capable of containing multiple servers.

    .PARAMETER ConnString
    Connection string for the target SQL Server database
    
    .PARAMETER BatchSize
    The records maximum batch size when performing the SQL Server bulk import command.

    #>
    
param( 
    $PerfmonDirectory,
    $ServerName,
    $ConnString = "Data Source=targetserver;Initial Catalog=PerfmonImport;Connection Timeout=1200;Integrated Security=True;Application Name=Heraflux_BLGtoSQL;",
    $BatchSize = 50000
)

Clear-Host

#Find all CABs in specified folder so we can unpack
$Files = get-childitem $PerfmonDir -recurse 
$CabList = $Files | where {$_.extension -eq ".cab"} 
ForEach ($File in $CabList) {
   Write-Host "CAB file to extract: " $File.FullName
   Write-Host "CAB file to extract into folder: " $File.DirectoryName
   expand -F:* $File.FullName $File.DirectoryName
}

#Find all BLG files and RELOG to CSV
#Relog with TSV format instead of CSV because of Counters that have commas in them
#Example: \\DBASE\Processor Information(1,4)\% of Maximum Frequency
$Files = get-childitem $PerfmonDir -recurse 
$BLGList = $Files | where {$_.extension -eq ".blg"} 
ForEach ($File in $BLGList) {
   Write-Host "BLG file to relog: " $File.FullName
   Write-Host "BLG file to relog into folder: " $File.DirectoryName
   $CSVName = $File.FullName -replace ".blg",".csv"
   relog $File.FullName -f tsv -y -o $CSVName
}

#Find all CSV files and import into database
$Files = get-childitem $PerfmonDir -recurse 
$BLGList = $Files | where {$_.extension -eq ".csv"} 
ForEach ($File in $BLGList) {
   Write-Host "CSV file to import: " $File.FullName
   $conn = new-object System.Data.SqlClient.SqlConnection($ConnString)
   $bcp = new-object ("System.Data.SqlClient.SqlBulkCopy") $conn
   $bcp.DestinationTableName = "dbo.PerfmonImportStage"
   $bcp.BulkCopyTimeout = 0
   $conn.Open()
   
   #Create placeholder datatable
   $dt = new-object System.Data.DataTable
   $col0 = new-object System.Data.DataColumn 'ServerName' 
   $col1 = new-object System.Data.DataColumn 'DateTimeStamp' 
   $col2 = new-object System.Data.DataColumn 'CounterInstance'
   $col3 = new-object System.Data.DataColumn 'CounterValue' 
   $dt.columns.Add($col0) 
   $dt.columns.Add($col1)
   $dt.columns.Add($col2)
   $dt.columns.Add($col3)

   $datapointCounter = 0

   #Load this CSV into RAM
   $csv = Import-Csv -Delimiter "`t" -Path $File.FullName

   #Iterate through the CSV
   foreach($line in $csv) {
        $properties = $line | Get-Member -MemberType Properties
        #Iterate through columns
        for($i=0; $i -lt $properties.Count; $i++) {
            $timestamp = $line | select -expandproperty $properties[0].Name
            $col = $properties[$i]
            $colvalue = $line | select -expandproperty $col.Name

            #Skip header row and empty datapoints
            if ( !([string]$col.Name -like "*SV *") -And ([string]$colvalue.trim().length -gt 0) ) {
                try {
                    $row = $dt.NewRow()
                    $row.ServerName = $ServerName
                    $row.DateTimeStamp = [datetime]$timestamp
                    $row.CounterInstance = [string]$col.Name
                    $row.CounterValue = [float]$colvalue.trim()
                    $dt.Rows.Add($row)
                } catch {
                    Write-Host "Invalid counter name: " $col.Name
                }
            }
            $datapointCounter = $datapointCounter + 1

            #Flush to database after batch size is met
            if ( ($datapointCounter % $BatchSize) -eq 0 ) {
                Write-Host $datapointCounter "points collected. Flushing to database..."
                $bcp.WriteToServer($dt)
                $dt.Clear()
            }
        }
   }

   #Final flush to database
   Write-Host $datapointCounter "points collected. Flushing to database..."
   $bcp.WriteToServer($dt)
   $dt.Clear()

   #Clean up
   $conn.Close()
   $conn.Dispose()
   $bcp.Close()
   $bcp.Dispose()
   $dt.Dispose()
   [System.GC]::Collect()

   
}


#Move data to final table and clean up staging table
Write-Host "Now moving data to the final table..."
$conn = new-object System.Data.SqlClient.SqlConnection($ConnString)
$conn.Open()
$cmd = new-object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$cmd.CommandText = "INSERT INTO dbo.PerfmonImport (ServerName, DateTimeStamp, CounterInstance, CounterValue) SELECT ServerName, DateTimeStamp, CounterInstance, CounterValue from dbo.PerfmonImportStage"
$cmd.ExecuteNonQuery()
$cmd.CommandText = "TRUNCATE TABLE dbo.PerfmonImportStage"
$cmd.ExecuteNonQuery()
$conn.Close()
Write-Host "Done importing this Perfmon data batch!"