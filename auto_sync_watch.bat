@echo off
setlocal EnableDelayedExpansion

title Data Launchpad Launcher
:: ============================================================================
:: CONFIGURATION
:: ============================================================================

:: Path to Repository 1 (The folder containing .git for V2)
set "REPO_1=C:\Users\Staff\Dropbox\Projects\Work\data_pipeline_automation_V2"

:: Path to Repository 2 (The folder containing .git for V3)
set "REPO_2=C:\Users\Staff\Dropbox\Projects\Work\data_pipeline_automation_V3"

:: How often to check for changes (in seconds)
set "INTERVAL=10"

:: ============================================================================
:: MAIN LOOP
:: ============================================================================
echo [%DATE% %TIME%] Auto-Sync Watcher Started.
echo Monitoring:
echo 1. %REPO_1%
echo 2. %REPO_2%
echo.

:loop
    :: 1. Sync changes from V2 to V3 (Overwrites V3 files with V2 versions)
    :: /E=Recursive, /XO=ExcludeOlder (only copy if V2 is newer), /XD=Exclude dirs
    robocopy "%REPO_1%" "%REPO_2%" /E /XO /XD .git .vs __pycache__ .idea /R:1 /W:1 >nul

    call :process_repo "%REPO_1%" "Repo 1"
    call :process_repo "%REPO_2%" "Repo 2"
    
    :: Wait before next check
    timeout /t %INTERVAL% /nobreak >nul
goto :loop

:: ============================================================================
:: SUBROUTINE: Process a Repository
:: ============================================================================
:process_repo
set "TARGET_DIR=%~1"
set "LABEL=%~2"

if not exist "%TARGET_DIR%\.git" goto :eof

pushd "%TARGET_DIR%"

:: Check for changes (git status --porcelain returns output if changes exist)
set "HAS_CHANGES="
for /f "tokens=*" %%i in ('git status --porcelain') do set "HAS_CHANGES=1"

if defined HAS_CHANGES (
    echo [%DATE% %TIME%] Changes detected in %LABEL%. Syncing...
    
    git add .
    git commit -m "Auto-sync: %DATE% %TIME%"
    git push
    
    echo [%DATE% %TIME%] %LABEL% pushed.
)

popd
goto :eof