@echo off
setlocal enabledelayedexpansion

for /f "tokens=1,2 delims==" %%a in ('wmic path Win32_LocalTime get Day^,Month^,Year /format:list ^| findstr "="') do (
    set "%%a=%%b"
)

:: Zero-pad day and month
if 1%Day% LSS 20 set Day=0%Day%
if 1%Month% LSS 20 set Month=0%Month%

set foldername=%Year%%Month%%Day%

mkdir "%foldername%"
echo Created folder: %foldername%
exit