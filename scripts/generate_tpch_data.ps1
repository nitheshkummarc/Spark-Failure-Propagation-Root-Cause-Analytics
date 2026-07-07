# ============================================================================
# TPC-H Data Generation Script for Windows
# Generates TPC-H benchmark data using dbgen
# ============================================================================

param(
    [int]$ScaleFactor = 25,  # 1GB by default (use 10 for 10GB)
    [string]$OutputDir = ".\tpch_data"
)

Write-Host "=============================================="
Write-Host "TPC-H Data Generation for Windows"
Write-Host "=============================================="
Write-Host "Scale Factor: ${ScaleFactor}GB"
Write-Host "Output Directory: ${OutputDir}"
Write-Host "=============================================="

# Create output directory
if (!(Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "Created output directory: $OutputDir"
}

# Check for dbgen
$dbgenPath = ".\dbgen.exe"
$tpchDir = ".\tpch-dbgen"

if (!(Test-Path $dbgenPath)) {
    Write-Host ""
    Write-Host "dbgen.exe not found. You have two options:"
    Write-Host ""
    Write-Host "OPTION 1: Download pre-compiled Windows binary"
    Write-Host "  1. Download from: https://github.com/electrum/tpch-dbgen"
    Write-Host "  2. Compile using Visual Studio or MinGW"
    Write-Host "  3. Place dbgen.exe in the scripts folder"
    Write-Host ""
    Write-Host "OPTION 2: Use WSL (Windows Subsystem for Linux)"
    Write-Host "  1. Open WSL terminal"
    Write-Host "  2. Run: git clone https://github.com/electrum/tpch-dbgen.git"
    Write-Host "  3. Run: cd tpch-dbgen && make"
    Write-Host "  4. Run: ./dbgen -s $ScaleFactor -f"
    Write-Host ""
    Write-Host "OPTION 3: Generate sample data (for testing)"
    Write-Host "  This script will generate a small sample dataset for testing."
    Write-Host ""
    
    $response = Read-Host "Generate sample data for testing? (Y/N)"
    
    if ($response -eq "Y" -or $response -eq "y") {
        Write-Host ""
        Write-Host "Generating sample TPC-H data..."
        
        # Generate sample data files
        Generate-SampleData -OutputDir $OutputDir
        
        Write-Host ""
        Write-Host "Sample data generated successfully!"
        Write-Host "Note: For production, use the real dbgen tool."
    } else {
        Write-Host ""
        Write-Host "Please install dbgen and run this script again."
        exit 1
    }
} else {
    # Run dbgen
    Write-Host "Running dbgen..."
    Push-Location (Split-Path $dbgenPath)
    & $dbgenPath -s $ScaleFactor -f -v
    
    # Move generated files
    Get-ChildItem -Filter "*.tbl" | Move-Item -Destination $OutputDir -Force
    Pop-Location
}

# Show results
Write-Host ""
Write-Host "Generated files:"
Get-ChildItem $OutputDir -Filter "*.tbl" | Format-Table Name, @{N='Size(MB)';E={[math]::Round($_.Length/1MB, 2)}}

$totalSize = (Get-ChildItem $OutputDir -Filter "*.tbl" | Measure-Object -Property Length -Sum).Sum / 1MB
Write-Host "=============================================="
Write-Host "Total size: $([math]::Round($totalSize, 2)) MB"
Write-Host "=============================================="

# Function to generate sample data
function Generate-SampleData {
    param([string]$OutputDir)
    
    # Sample nation data
    $nationData = @"
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
6|FRANCE|3|refully final requests. regular, ironi
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
9|INDONESIA|2|slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
10|IRAN|4|efully alongside of the slyly final dependencies. 
11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula
12|JAPAN|2|ously. final, express gifts cajole a
13|JORDAN|4|ic deposits are blithely about the carefully regular pa
14|KENYA|0|pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t
15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?
16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r
17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun
18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos
19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account
20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely
21|VIETNAM|2|hely enticingly express accounts. even, final 
22|RUSSIA|3|requests against the platelets use never according to the quickly regular pint
23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull
24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be
"@
    $nationData | Out-File -FilePath "$OutputDir\nation.tbl" -Encoding ASCII
    
    # Sample region data
    $regionData = @"
0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to 
1|AMERICA|hs use ironic, even requests. s
2|ASIA|ges. thinly even pinto beans ca
3|EUROPE|ly final courts cajole furiously final excuse
4|MIDDLE EAST|uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl
"@
    $regionData | Out-File -FilePath "$OutputDir\region.tbl" -Encoding ASCII
    
    # Sample customer data (100 rows)
    $customerData = ""
    for ($i = 1; $i -le 100; $i++) {
        $nationKey = Get-Random -Minimum 0 -Maximum 25
        $acctBal = [math]::Round((Get-Random -Minimum -999.99 -Maximum 9999.99), 2)
        $segments = @("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
        $segment = $segments[(Get-Random -Minimum 0 -Maximum 5)]
        $customerData += "$i|Customer#$('{0:D9}' -f $i)|Address$i|$nationKey|25-$((Get-Random -Minimum 100 -Maximum 999))-$((Get-Random -Minimum 100 -Maximum 999))-$((Get-Random -Minimum 1000 -Maximum 9999))|$acctBal|$segment|comment for customer $i|`n"
    }
    $customerData | Out-File -FilePath "$OutputDir\customer.tbl" -Encoding ASCII
    
    # Sample supplier data (10 rows)
    $supplierData = ""
    for ($i = 1; $i -le 10; $i++) {
        $nationKey = Get-Random -Minimum 0 -Maximum 25
        $acctBal = [math]::Round((Get-Random -Minimum -999.99 -Maximum 9999.99), 2)
        $supplierData += "$i|Supplier#$('{0:D9}' -f $i)|Address$i|$nationKey|25-$((Get-Random -Minimum 100 -Maximum 999))-$((Get-Random -Minimum 100 -Maximum 999))-$((Get-Random -Minimum 1000 -Maximum 9999))|$acctBal|comment for supplier $i|`n"
    }
    $supplierData | Out-File -FilePath "$OutputDir\supplier.tbl" -Encoding ASCII
    
    # Sample part data (200 rows)
    $partData = ""
    $types = @("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO")
    $containers = @("SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "LG CASE", "LG BOX")
    for ($i = 1; $i -le 200; $i++) {
        $size = Get-Random -Minimum 1 -Maximum 50
        $retailPrice = [math]::Round(900 + ($i / 10), 2)
        $type = $types[(Get-Random -Minimum 0 -Maximum 6)]
        $container = $containers[(Get-Random -Minimum 0 -Maximum 8)]
        $partData += "$i|Part$i|Manufacturer#$((Get-Random -Minimum 1 -Maximum 5))|Brand#$((Get-Random -Minimum 1 -Maximum 5))$((Get-Random -Minimum 1 -Maximum 5))|$type BRASS|$size|$container|$retailPrice|comment $i|`n"
    }
    $partData | Out-File -FilePath "$OutputDir\part.tbl" -Encoding ASCII
    
    # Sample partsupp data
    $partsuppData = ""
    for ($partKey = 1; $partKey -le 200; $partKey++) {
        for ($suppKey = 1; $suppKey -le 4; $suppKey++) {
            $availQty = Get-Random -Minimum 1 -Maximum 9999
            $supplyCost = [math]::Round((Get-Random -Minimum 1.00 -Maximum 1000.00), 2)
            $partsuppData += "$partKey|$suppKey|$availQty|$supplyCost|comment for part $partKey supp $suppKey|`n"
        }
    }
    $partsuppData | Out-File -FilePath "$OutputDir\partsupp.tbl" -Encoding ASCII
    
    # Sample orders data (1500 rows)
    $ordersData = ""
    $orderPriorities = @("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
    $orderStatus = @("F", "O", "P")
    for ($i = 1; $i -le 1500; $i++) {
        $custKey = Get-Random -Minimum 1 -Maximum 100
        $status = $orderStatus[(Get-Random -Minimum 0 -Maximum 3)]
        $totalPrice = [math]::Round((Get-Random -Minimum 1000.00 -Maximum 500000.00), 2)
        $year = Get-Random -Minimum 1992 -Maximum 1998
        $month = Get-Random -Minimum 1 -Maximum 13
        $day = Get-Random -Minimum 1 -Maximum 29
        $orderDate = "$year-$('{0:D2}' -f $month)-$('{0:D2}' -f $day)"
        $priority = $orderPriorities[(Get-Random -Minimum 0 -Maximum 5)]
        $clerk = "Clerk#$('{0:D9}' -f (Get-Random -Minimum 1 -Maximum 1000))"
        $shipPriority = Get-Random -Minimum 0 -Maximum 1
        $ordersData += "$i|$custKey|$status|$totalPrice|$orderDate|$priority|$clerk|$shipPriority|comment for order $i|`n"
    }
    $ordersData | Out-File -FilePath "$OutputDir\orders.tbl" -Encoding ASCII
    
    # Sample lineitem data (6000 rows)
    $lineitemData = ""
    $shipModes = @("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB")
    $shipInstructs = @("DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN")
    $returnFlags = @("N", "R", "A")
    $lineStatus = @("O", "F")
    
    for ($orderKey = 1; $orderKey -le 1500; $orderKey++) {
        $numLines = Get-Random -Minimum 1 -Maximum 7
        for ($lineNum = 1; $lineNum -le $numLines; $lineNum++) {
            $partKey = Get-Random -Minimum 1 -Maximum 200
            $suppKey = Get-Random -Minimum 1 -Maximum 10
            $quantity = Get-Random -Minimum 1 -Maximum 50
            $extendedPrice = [math]::Round($quantity * (Get-Random -Minimum 10.00 -Maximum 100.00), 2)
            $discount = [math]::Round((Get-Random -Minimum 0.00 -Maximum 0.10), 2)
            $tax = [math]::Round((Get-Random -Minimum 0.00 -Maximum 0.08), 2)
            $returnFlag = $returnFlags[(Get-Random -Minimum 0 -Maximum 3)]
            $status = $lineStatus[(Get-Random -Minimum 0 -Maximum 2)]
            
            $year = Get-Random -Minimum 1992 -Maximum 1998
            $month = Get-Random -Minimum 1 -Maximum 13
            $day = Get-Random -Minimum 1 -Maximum 29
            $shipDate = "$year-$('{0:D2}' -f $month)-$('{0:D2}' -f $day)"
            $commitDate = "$year-$('{0:D2}' -f $month)-$('{0:D2}' -f (Get-Random -Minimum 1 -Maximum 29))"
            $receiptDate = "$year-$('{0:D2}' -f $month)-$('{0:D2}' -f (Get-Random -Minimum 1 -Maximum 29))"
            
            $shipInstruct = $shipInstructs[(Get-Random -Minimum 0 -Maximum 4)]
            $shipMode = $shipModes[(Get-Random -Minimum 0 -Maximum 7)]
            
            $lineitemData += "$orderKey|$partKey|$suppKey|$lineNum|$quantity|$extendedPrice|$discount|$tax|$returnFlag|$status|$shipDate|$commitDate|$receiptDate|$shipInstruct|$shipMode|comment $orderKey-$lineNum|`n"
        }
    }
    $lineitemData | Out-File -FilePath "$OutputDir\lineitem.tbl" -Encoding ASCII
    
    Write-Host "Generated sample TPC-H tables:"
    Write-Host "  - nation.tbl (25 rows)"
    Write-Host "  - region.tbl (5 rows)"
    Write-Host "  - customer.tbl (100 rows)"
    Write-Host "  - supplier.tbl (10 rows)"
    Write-Host "  - part.tbl (200 rows)"
    Write-Host "  - partsupp.tbl (800 rows)"
    Write-Host "  - orders.tbl (1500 rows)"
    Write-Host "  - lineitem.tbl (~6000 rows)"
}

# Run sample data generation if no dbgen
if (!(Test-Path $dbgenPath)) {
    Generate-SampleData -OutputDir $OutputDir
}
