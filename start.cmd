@echo off
rem Double-click this file to start the system.
rem (Double-clicking start.ps1 directly opens it in Notepad instead of running it.)
rem Pass -Simple to run without Docker:  start.cmd -Simple
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
if errorlevel 1 pause
