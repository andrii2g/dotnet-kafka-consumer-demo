param(
    [string]$BootstrapServers = "localhost:9092",
    [string]$Topic = "",
    [int]$Count = 100000,
    [int]$CommitBatch = 1000,
    [int]$PollTimeoutMs = 100,
    [string]$RuntimeIdentifier = "win-x64",
    [switch]$SkipBuild,
    [switch]$SkipClean,
    [switch]$StartDocker,
    [switch]$CleanupDocker,
    [ValidateSet("none", "local", "all")]
    [string]$CleanupImages = "none",
    [string]$ComposeFile = "docker/docker-compose.yml",
    [string]$ReportDir = "reports"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$composePath = Join-Path $repoRoot $ComposeFile
if ([string]::IsNullOrWhiteSpace($Topic)) {
    $Topic = "orders.created.bench.$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))"
}

function Test-TcpPort {
    param([string]$HostName, [int]$Port)

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $connectTask = $client.ConnectAsync($HostName, $Port)
        if (-not $connectTask.Wait([TimeSpan]::FromSeconds(2))) {
            return $false
        }

        return $client.Connected
    }
    finally {
        $client.Dispose()
    }
}

function Invoke-Process {
    param([string[]]$Command)

    Write-Host "> $($Command -join ' ')"
    & $Command[0] @($Command | Select-Object -Skip 1)
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Capture {
    param([string[]]$Command)

    Write-Host "> $($Command -join ' ')"
    $output = & $Command[0] @($Command | Select-Object -Skip 1)
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }
    if ($exitCode -ne 0) {
        throw "Command failed with exit code $exitCode"
    }

    return ($output -join [Environment]::NewLine)
}

function Wait-Kafka {
    param([string]$BootstrapServers)

    $bootstrapParts = $BootstrapServers.Split(":")
    if ($bootstrapParts.Length -lt 2) {
        throw "Invalid bootstrap servers value: $BootstrapServers"
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    do {
        if (Test-TcpPort $bootstrapParts[0] ([int]$bootstrapParts[1])) {
            return
        }

        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)

    throw "Kafka is not reachable at $BootstrapServers."
}

function Stop-DockerStack {
    if (-not $CleanupDocker) {
        return
    }

    $command = @("docker", "compose", "-f", $composePath, "down", "--volumes", "--remove-orphans")
    if ($CleanupImages -ne "none") {
        $command += @("--rmi", $CleanupImages)
    }

    try {
        Invoke-Process $command
    }
    catch {
        Write-Warning $_
    }
}

try {
    if ($StartDocker) {
        Invoke-Process @("docker", "compose", "-f", $composePath, "up", "-d")
    }

    Wait-Kafka $BootstrapServers

    if (-not $SkipBuild) {
        if (-not $SkipClean) {
            Get-ChildItem -Path "$repoRoot\src", "$repoRoot\tests" -Directory -Recurse -Include bin, obj |
                Remove-Item -Recurse -Force
        }

        Invoke-Process @("dotnet", "restore", "$repoRoot\DotNetKafkaConsumerLab.slnx")
        Invoke-Process @("dotnet", "build", "$repoRoot\DotNetKafkaConsumerLab.slnx", "--configuration", "Release", "--no-restore", "-v", "minimal")
        Invoke-Process @("dotnet", "publish", "$repoRoot\src\KafkaConsumerLab.AotProducer\KafkaConsumerLab.AotProducer.csproj", "-c", "Release", "-r", $RuntimeIdentifier, "--no-restore", "-v", "minimal")
        Invoke-Process @("dotnet", "publish", "$repoRoot\src\KafkaConsumerLab.AotConsumer\KafkaConsumerLab.AotConsumer.csproj", "-c", "Release", "-r", $RuntimeIdentifier, "--no-restore", "-v", "minimal")
    }

    $producerExe = "$repoRoot\src\KafkaConsumerLab.AotProducer\bin\Release\net10.0\$RuntimeIdentifier\publish\KafkaConsumerLab.AotProducer.exe"
    $aotConsumerExe = "$repoRoot\src\KafkaConsumerLab.AotConsumer\bin\Release\net10.0\$RuntimeIdentifier\publish\KafkaConsumerLab.AotConsumer.exe"
    $jitConsumerDll = "$repoRoot\src\KafkaConsumerLab.AotConsumer\bin\Release\net10.0\$RuntimeIdentifier\KafkaConsumerLab.AotConsumer.dll"

    $eventPrefix = "bench-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))"
    $producerOutput = Invoke-Capture @(
        $producerExe,
        "--bootstrap", $BootstrapServers,
        "--topic", $Topic,
        "--count", $Count,
        "--event-prefix", $eventPrefix,
        "--create-topic", "true",
        "--partitions", "1",
        "--replication-factor", "1"
    ) | Out-Null

    $jitGroup = "bench-jit-$([Guid]::NewGuid().ToString('N'))"
    $aotGroup = "bench-aot-$([Guid]::NewGuid().ToString('N'))"

    $jitOutput = Invoke-Capture @(
        "dotnet",
        $jitConsumerDll,
        "--bootstrap", $BootstrapServers,
        "--topic", $Topic,
        "--group-id", $jitGroup,
        "--count", $Count,
        "--commit-batch", $CommitBatch,
        "--from-beginning", "true",
        "--deserialize", "true",
        "--poll-timeout-ms", $PollTimeoutMs
    )

    $aotOutput = Invoke-Capture @(
        $aotConsumerExe,
        "--bootstrap", $BootstrapServers,
        "--topic", $Topic,
        "--group-id", $aotGroup,
        "--count", $Count,
        "--commit-batch", $CommitBatch,
        "--from-beginning", "true",
        "--deserialize", "true",
        "--poll-timeout-ms", $PollTimeoutMs
    )

    Write-Host ""
    Write-Host "Benchmark topic: $Topic"
    Write-Host "JIT consumer: $jitOutput"
    Write-Host "AOT consumer: $aotOutput"

    $reportRoot = Join-Path $repoRoot $ReportDir
    New-Item -ItemType Directory -Path $reportRoot -Force | Out-Null
    $reportFile = Join-Path $reportRoot "benchmark-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss')).md"

    @"
# Kafka Consumer Benchmark

Generated at: $([DateTimeOffset]::UtcNow.ToString("O"))

## Configuration

| Setting | Value |
| --- | --- |
| Bootstrap servers | ``$BootstrapServers`` |
| Topic | ``$Topic`` |
| Message count | ``$Count`` |
| Commit batch | ``$CommitBatch`` |
| Poll timeout ms | ``$PollTimeoutMs`` |
| Runtime identifier | ``$RuntimeIdentifier`` |
| Start Docker | ``$StartDocker`` |
| Cleanup Docker | ``$CleanupDocker`` |
| Cleanup images | ``$CleanupImages`` |

## Producer

``````text
$producerOutput
``````

## CoreCLR/JIT Consumer

``````text
$jitOutput
``````

## Native AOT Consumer

``````text
$aotOutput
``````

## Commands

``````powershell
dotnet "$jitConsumerDll" --bootstrap "$BootstrapServers" --topic "$Topic" --group-id "$jitGroup" --count "$Count" --commit-batch "$CommitBatch" --from-beginning true --deserialize true --poll-timeout-ms "$PollTimeoutMs"
& "$aotConsumerExe" --bootstrap "$BootstrapServers" --topic "$Topic" --group-id "$aotGroup" --count "$Count" --commit-batch "$CommitBatch" --from-beginning true --deserialize true --poll-timeout-ms "$PollTimeoutMs"
``````
"@ | Set-Content -Path $reportFile -Encoding UTF8

    Write-Host "Report: $reportFile"
}
finally {
    Stop-DockerStack
}
