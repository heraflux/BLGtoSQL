<# 
    .SYNOPSIS 
    Perfmon BLG to SQL Server Importer - by David Klee and Bob Pusateri, Heraflux Technologies
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
    
    Sample usage of this script 
        ./BLGToSQL.ps1 -PerfmonDirectory "E:\PerfmonData" -ServerName="PRDSQL01" 
        
    .LINK
    Fetch this script it its most updated from at:
    https://github.com/heraflux/BLGtoSQL  
    
    .PARAMETER PerfmonDirectory
    The location of the Perfmon BLG file folder.

    .PARAMETER ServerName
    The server name this script is collecting from, so that the target database is capable of containing multiple servers.

    .PARAMETER ConnString
    Connection string for the target SQL Server database

    #>

[CmdletBinding()]    
param( 
    $PerfmonDirectory="c:\temp\perfmon",
    $ServerName="localhost",
    $ConnString=""
)
$VerbosePreference="Continue"

if($ConnString -eq "") {
    $ConnString = "Data Source=localhost;Initial Catalog=PerfmonImport;Integrated Security=True;Application Name=PowerShell_PerfmonToSQL;"
}

Clear-Host

$ErrorActionPreference = 'Stop';

# files in directory?
if(!(Test-Path -Path $PerfmonDirectory)) {
    throw "Directory $PerfmonDirectory does not exist.";
}

# connect to server
try {
    $sqlconn = new-object System.Data.SqlClient.SqlConnection($ConnString)
    $sqlconn.Open();
    $sqlconn.Close();
} catch {
    throw "Cannot connect to server $serverName.";
}

# does database "PerfmonImport" exist?
$ConnString = $ConnString.Replace("tempdb","PerfmonImport")
try {
    $sqlconn = new-object System.Data.SqlClient.SqlConnection($ConnString)
    $sqlconn.Open();
    $sqlconn.Close();
} catch {
    throw "Cannot connect to database [PerfmonImport]. Did you run DBSchema.sql?";
}



#Find all CABs in specified folder so we can unpack
Write-Verbose   "Searching for and extacting all CAB files in $PerfmonDirectory"
get-childitem $PerfmonDirectory -recurse -Filter "*.cab"  | % { expand -F:* $_.FullName  $_.DirectoryName } | out-null
Write-Verbose   "relogging for and extacting all CAB files in $PerfmonDirectory"
get-childitem $PerfmonDirectory -recurse -Filter "*.blg"  | % { relog $_.FullName -f tsv -y -o ($_.FullName -Replace (".blg",".csv")) } | out-null

write-verbose "Import Directory: $PerfmonDirectory"
#Find all BLG files and import into database
ForEach ($File in (get-childitem $PerfmonDirectory -recurse -Filter "*.csv")) {
   write-verbose "CSV file to import: $File"
   $bcp = new-object ("System.Data.SqlClient.SqlBulkCopy") $sqlconn
   $bcp.DestinationTableName = "dbo.PerfmonImport"
   $bcp.BulkCopyTimeout = 0
   $sqlconn.Open()

   #Create placeholder datatable
   $dt = new-object System.Data.DataTable
   $col0 = new-object System.Data.DataColumn 'ServerName' 
   $col1 = new-object System.Data.DataColumn 'DateTimeStamp' 
   $col2 = new-object System.Data.DataColumn 'CounterSet'
   $col3 = new-object System.Data.DataColumn 'CounterName'
   $col4 = new-object System.Data.DataColumn 'CounterInstance'
   $col5 = new-object System.Data.DataColumn 'CounterValue' 
   $dt.columns.Add($col0) 
   $dt.columns.Add($col1)
   $dt.columns.Add($col2)
   $dt.columns.Add($col3)
   $dt.columns.Add($col4)
   $dt.columns.Add($col5)

   #Map columns
   $bcp.ColumnMappings.Add('ServerName', 'ServerName')
   $bcp.ColumnMappings.Add('DateTimeStamp', 'DateTimeStamp')
   $bcp.ColumnMappings.Add('CounterSet', 'CounterSet')
   $bcp.ColumnMappings.Add('CounterName', 'CounterName')
   $bcp.ColumnMappings.Add('CounterInstance', 'CounterInstance')
   $bcp.ColumnMappings.Add('CounterValue', 'CounterValue')
   
   #Load this BLG into RAM
   try {
       $blg = Import-Counter -Path $File.FullName -ErrorAction SilentlyContinue

       #Iterate through the BLG's
       foreach($line in $blg) {
            #Iterate through columns
            $clist = $line.CounterSamples
            foreach ($sample in $clist){
                try {
                    $row = $dt.NewRow()
                    $s1 = ($sample.Path).Substring(2,$sample.Path.Length - 2)
                    $ServerName = $s1.Substring(0, $s1.IndexOf("\"))
                    $MainString = $s1.Substring($s1.IndexOf("\") , $s1.length - $s1.IndexOf("\"))
                    $CounterArray = $MainString -split '\\' 
                    $row.CounterName = $CounterArray[2]
                    $row.CounterSet = $CounterArray[1]
                    $row.ServerName = $ServerName
                    $row.DateTimeStamp = [datetime]$sample.Timestamp
                    $row.CounterInstance = [string]$sample.InstanceName
                    $row.CounterValue = [long]$sample.CookedValue
                    $dt.Rows.Add($row)
                    
                } catch {
                    write-error "Unknown Error Encountered"
                    Write-Error $_
                }
            }
        }
        $flushoutput = $blg.Count.ToString() + " points collected. Storing to database..."
        write-verbose $flushoutput
        try {
            $bcp.WriteToServer($dt)
        } finally {
            $dt.Clear()
        }
    } catch {
        Write-Error "Error Processing CSV file $File"
    } finally {
        $sqlconn.Close();
    }
} # for each file

#Clean up
$sqlconn.Dispose()
$bcp.Close()
$bcp.Dispose()
$dt.Dispose()
[System.GC]::Collect()

   

write-verbose "Done importing this Perfmon data batch!"
