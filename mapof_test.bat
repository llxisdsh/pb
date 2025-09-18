@echo off
setlocal

:build
set TIMESTAMP=%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%_%TIME:~0,2%-%TIME:~3,2%-%TIME:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%
set OUTPUT=mapof_test_%TIMESTAMP%.exe

go test mapof_test.go mapof_go1.23_test.go mapof_go1.24_test.go mapof.go ^
mapof_deprecated.go ^
mapof_experimental.go ^
mapof_opt_cachelinesize.go ^
mapof_opt_enablepadding_off.go ^
mapof_opt_embeddedhash_off.go ^
atomic_utils.go ^
 -c -o "%OUTPUT%"
 
if %ERRORLEVEL% neq 0 (
    echo Error build.
    pause
    exit /b %ERRORLEVEL%
)

:loop
echo %TIME% Running test...

"%OUTPUT%"
if %ERRORLEVEL% neq 0 (
    echo Error encountered. Stopping the loop.
    pause
    exit /b %ERRORLEVEL%
)

::echo Test completed successfully. Looping...
goto loop