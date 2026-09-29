# start-redis.ps1 - start Redis for the LADWP migration (no admin required)
# Redis binaries must be at C:\Redis\ (already installed by setup-redis-windows.ps1)
# Run from any PowerShell window: .\start-redis.ps1

$redisSrv  = 'C:\Redis\redis-server.exe'
$redisCli  = 'C:\Redis\redis-cli.exe'
$redisConf = 'C:\Redis\redis.conf'

if (-not (Test-Path $redisSrv)) {
    Write-Error "Redis not found at C:\Redis\. Run setup-redis-windows.ps1 first."
    exit 1
}

# Kill any stale instance
Stop-Process -Name redis-server -Force -ErrorAction SilentlyContinue

# Write config if missing (no BOM so Redis can parse it)
if (-not (Test-Path $redisConf)) {
    $conf = @"
bind 127.0.0.1
port 6379
protected-mode no
appendonly yes
appendfsync everysec
appendfilename appendonly.aof
save ""
logfile C:/Redis/redis.log
dir C:/Redis
"@
    [System.IO.File]::WriteAllText($redisConf, $conf, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Config written to $redisConf"
}

Write-Host "Starting Redis..." -ForegroundColor Cyan
$proc = Start-Process -FilePath $redisSrv -ArgumentList $redisConf -NoNewWindow -PassThru
Start-Sleep -Seconds 2

$pong = & $redisCli -p 6379 PING 2>$null
if ($pong -eq 'PONG') {
    Write-Host "Redis is running (PID $($proc.Id)) - 127.0.0.1:6379" -ForegroundColor Green
} else {
    Write-Warning "Redis did not respond to PING. Check C:\Redis\redis.log"
}
