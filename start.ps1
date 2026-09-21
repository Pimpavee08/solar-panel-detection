<#
.SYNOPSIS
  เปิดระบบตรวจจับแผงโซลาร์เซลล์ครบชุดด้วยคำสั่งเดียว

.DESCRIPTION
  แบบปกติ     เปิด Docker Desktop -> Airflow + PostgreSQL -> เว็บที่พอร์ต 8009 -> เบราว์เซอร์
  แบบ -Simple ไม่ใช้ Docker เว็บรันงานเองและเก็บข้อมูลใน SQLite ที่พอร์ต 8008

  ปิดระบบ: กด Ctrl+C ในหน้าต่างนี้เพื่อปิดเว็บ
           ถ้าจะปิด Airflow และ PostgreSQL ด้วย ให้สั่ง  docker compose down

.EXAMPLE
  .\start.ps1

.EXAMPLE
  .\start.ps1 -Simple
#>
param(
  # ไม่ใช้ Docker/Airflow รันเว็บกับ SQLite อย่างเดียว
  [switch]$Simple,
  # ไม่ต้องเปิดเบราว์เซอร์ให้ (ใช้ตอนทดสอบสคริปต์)
  [switch]$NoBrowser
)

# ไม่ตั้ง $ErrorActionPreference = 'Stop' เพราะใน Windows PowerShell 5.1 ข้อความที่
# โปรแกรมภายนอก (docker, python) เขียนออกทาง stderr จะถูกมองเป็น error แล้วหยุด
# สคริปต์ทั้งที่โปรแกรมทำงานสำเร็จ จึงตรวจ $LASTEXITCODE เองทุกครั้งแทน

# ให้ path สัมพัทธ์ (data/inference, ฐานข้อมูล SQLite) ชี้ถูกที่ ไม่ว่าจะรันจากโฟลเดอร์ไหน
Set-Location -LiteralPath $PSScriptRoot

$AirflowUrl  = 'http://localhost:8080'
$PostgresUrl = 'postgresql+psycopg2://airflow:airflow@localhost:5432/solar'
$Port        = if ($Simple) { 8008 } else { 8009 }
$Url         = "http://localhost:$Port"

function Say([string]$text)  { Write-Host "  $text" }
function Ok([string]$text)   { Write-Host "  [OK] $text" -ForegroundColor Green }
function Step([string]$text) { Write-Host ''; Write-Host "> $text" -ForegroundColor Cyan }

function Fail([string]$text, [string]$hint = '') {
  Write-Host ''
  Write-Host "  [ผิดพลาด] $text" -ForegroundColor Red
  if ($hint) { Write-Host "  $hint" -ForegroundColor Yellow }
  exit 1
}

function Test-Url([string]$url) {
  try { $null = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 3; return $true }
  catch { return $false }
}

function Wait-Until([scriptblock]$check, [int]$seconds, [string]$label) {
  if (& $check) { return $true }
  Write-Host "  กำลังรอ $label " -NoNewline
  for ($i = 0; $i -lt $seconds; $i += 3) {
    Start-Sleep -Seconds 3
    Write-Host '.' -NoNewline
    if (& $check) { Write-Host ''; return $true }
  }
  Write-Host ''
  return $false
}

Write-Host ''
Write-Host '  Solar Panel Detection Pipeline' -ForegroundColor Yellow
if ($Simple) { Say 'โหมดง่าย: เว็บ + SQLite ไม่ใช้ Docker' }
else         { Say 'โหมดเต็ม: Docker + Airflow + PostgreSQL + เว็บ' }

# ---------------------------------------------------------------- Python
Step 'ตรวจ Python และไลบรารี'
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
  Fail 'ไม่พบคำสั่ง python' 'ติดตั้ง Python หรือ Anaconda แล้วเปิดหน้าต่างใหม่'
}

$modules = 'uvicorn, fastapi, sqlalchemy, rasterio, geopandas'
if (-not $Simple) { $modules += ', psycopg2' }
& python -c "import $modules" 2>$null
if ($LASTEXITCODE -ne 0) {
  Fail 'Python ยังขาดไลบรารีที่ต้องใช้' 'รันคำสั่งนี้ก่อน:  pip install -r requirements.txt psycopg2-binary'
}
Ok $python.Source

# ---------------------------------------------------------------- Docker + Airflow
if ($Simple) {
  # ล้างค่าที่อาจค้างจากการรันโหมดเต็มในหน้าต่างเดียวกัน ไม่งั้นจะไปต่อ Postgres โดยไม่ตั้งใจ
  Remove-Item Env:SOLAR_DATABASE_URL, Env:SOLAR_AIRFLOW_URL -ErrorAction SilentlyContinue
}
else {
  Step 'ตรวจ Docker'
  if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail 'ไม่พบโปรแกรม Docker' 'ติดตั้ง Docker Desktop หรือใช้  .\start.ps1 -Simple  แทน'
  }

  $dockerReady = { $null = docker info --format '{{.ServerVersion}}' 2>$null; $LASTEXITCODE -eq 0 }
  if (-not (& $dockerReady)) {
    $exe = @(
      "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe",
      "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    ) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

    if (-not $exe) {
      Fail 'Docker ยังไม่ทำงาน และหาโปรแกรม Docker Desktop ไม่เจอ' 'เปิด Docker Desktop เอง แล้วรันสคริปต์นี้ใหม่'
    }
    Say 'เปิด Docker Desktop ...'
    Start-Process -FilePath $exe
    Write-Host '  กำลังรอ Docker ' -NoNewline
    $ready = $false
    $relaunched = $false
    for ($t = 0; $t -lt 180; $t += 3) {
      Start-Sleep -Seconds 3
      Write-Host '.' -NoNewline
      if (& $dockerReady) { $ready = $true; break }
      # การเปิดครั้งแรกบางครั้งหลุดไปเองโดยไม่มีหน้าต่างขึ้น (เจอจริงระหว่างทดสอบ)
      # ถ้าผ่านไป 20 วินาทีแล้วยังไม่มีโปรเซสของ Docker Desktop เลย ให้เปิดซ้ำหนึ่งครั้ง
      if (-not $relaunched -and $t -ge 20 -and
          -not (Get-Process 'Docker Desktop' -ErrorAction SilentlyContinue)) {
        Start-Process -FilePath $exe
        $relaunched = $true
      }
    }
    Write-Host ''
    if (-not $ready) {
      Fail 'Docker ยังไม่พร้อมหลังรอ 3 นาที' 'ดูหน้าต่าง Docker Desktop ว่ามีให้กดยอมรับเงื่อนไขหรือไม่ แล้วรันสคริปต์นี้ใหม่'
    }
  }
  Ok 'Docker พร้อม'

  Step 'เปิด Airflow และ PostgreSQL'
  docker compose up -d
  if ($LASTEXITCODE -ne 0) {
    Fail 'docker compose up ไม่สำเร็จ' 'ดูข้อความด้านบน หรือรัน  docker compose logs  เพื่อดูสาเหตุ'
  }
  if (-not (Wait-Until { Test-Url "$AirflowUrl/health" } 240 'Airflow')) {
    Fail 'Airflow ไม่ตอบสนองหลังรอ 4 นาที' 'รัน  docker compose logs airflow-webserver  เพื่อดูสาเหตุ'
  }
  Ok "Airflow พร้อมที่ $AirflowUrl"

  # เว็บต้องใช้ฐานข้อมูลเดียวกับ Airflow ไม่งั้นจะมองไม่เห็นสถานะที่ Airflow เขียนลงไป
  $env:SOLAR_DATABASE_URL = $PostgresUrl
  $env:SOLAR_AIRFLOW_URL  = $AirflowUrl
}

# ---------------------------------------------------------------- เว็บ
Step 'เปิดเว็บ'
try { $health = Invoke-RestMethod "$Url/health" -UseBasicParsing -TimeoutSec 3 } catch { $health = $null }
if ($health) {
  if (-not $Simple -and $health.executor -ne 'airflow') {
    Fail "มีเว็บเปิดอยู่ที่ $Url แต่ไม่ได้ต่อกับ Airflow" 'ปิดหน้าต่างที่รันเว็บตัวนั้นก่อน แล้วรันสคริปต์นี้ใหม่'
  }
  Ok "เว็บเปิดอยู่แล้วที่ $Url"
  if (-not $NoBrowser) { Start-Process $Url }
  exit 0
}

# เปิดเบราว์เซอร์ให้เองทันทีที่เว็บพร้อม (เว็บต้องรันค้างอยู่ในหน้าต่างนี้ จึงให้งานนี้ทำเบื้องหลัง)
if (-not $NoBrowser) {
  $null = Start-Job -ArgumentList $Url -ScriptBlock {
    param($u)
    for ($i = 0; $i -lt 60; $i++) {
      try { $null = Invoke-WebRequest "$u/health" -UseBasicParsing -TimeoutSec 2; Start-Process $u; return }
      catch { Start-Sleep -Seconds 1 }
    }
  }
}

Write-Host ''
Write-Host "  เว็บ: $Url" -ForegroundColor Green
Write-Host '  กด Ctrl+C เพื่อปิดเว็บ' -ForegroundColor DarkGray
if (-not $Simple) {
  Write-Host '  Airflow กับ PostgreSQL จะยังรันต่อ ปิดด้วย  docker compose down' -ForegroundColor DarkGray
}
Write-Host ''

try {
  & python -m uvicorn app:app --host 127.0.0.1 --port $Port
}
finally {
  Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
}
