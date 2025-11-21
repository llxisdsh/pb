@echo off
setlocal

:loop
echo %TIME% Running test...

go test
if %ERRORLEVEL% neq 0 (
    echo Error encountered. Stopping the loop.
    exit /b %ERRORLEVEL%
)
goto loop