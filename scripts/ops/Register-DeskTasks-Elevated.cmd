@echo off
:: Double-click or run from an elevated prompt to register \TheDesk\ scheduled tasks.
cd /d C:\the-desk
powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\ops\Register-DeskTasks.ps1" -UserProfilePath "%USERPROFILE%"
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\ops\Register-DeskTasks.ps1" -Verify
echo.
pause
