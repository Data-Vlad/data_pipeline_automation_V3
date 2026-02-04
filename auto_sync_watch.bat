@echo off
setlocal EnableDelayedExpansion

title GitHub Updater
:: ============================================================================
:: CONFIGURATION
:: ============================================================================

:: Path to Repository 2 (The folder containing .git for V3)
set "REPO_2=C:\Users\Staff\Dropbox\Projects\Work\data_pipeline_automation_V3"

:: How often to check for changes (in seconds)
set "INTERVAL=10"

:: ============================================================================
:: MAIN LOOP
:: ============================================================================
echo [%DATE% %TIME%] Auto-Sync Watcher Started.
echo Monitoring:
echo %REPO_2%
echo.

:loop
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