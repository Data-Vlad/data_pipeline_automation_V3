@echo off
:error
setlocal

:: ============================================================================
:: Configuration
:: ============================================================================
title Data Launchpad Launcher
cd /d "%~dp0"

set "SCRIPT_DIR=%~dp0"
set "VENV_DIR=%SCRIPT_DIR%venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "PYTHONW_EXE=%VENV_DIR%\Scripts\pythonw.exe"
set "REQUIREMENTS_FILE=%SCRIPT_DIR%requirements.txt"
set "ENV_FILE=%SCRIPT_DIR%.env"
set "PYTHONPATH=%SCRIPT_DIR%"
set "UI_SCRIPT=%SCRIPT_DIR%simple_ui.py"
set "GET_CREDS_SCRIPT=%SCRIPT_DIR%get_credentials.py"
set "CREATE_DIRS_SCRIPT=%SCRIPT_DIR%create_dirs.py"
set "ERROR_LOG=%SCRIPT_DIR%launcher-error.log"
set "DAGSTER_HOME_DIR=%SCRIPT_DIR%dagster_home"

:: Clean up old log files
if exist "%ERROR_LOG%" del "%ERROR_LOG%"
if exist "simple_ui.log" del "simple_ui.log"


:: ============================================================================
:: Helper Subroutines for logging
:: ============================================================================

goto :main

:colorEcho
    echo %DEL% > "%~2.color"
    findstr /v /a:%1 /R "^$" "%~2.color" nul
    del "%~2.color"
    goto :eof

:log
    echo [%~1] %~2
    goto :eof

:handle_error
    call :log ERROR "%~1"
    echo [FATAL] %~1 >> "%ERROR_LOG%"
    echo.
    echo   +--------------------------------------------------------------+
    echo   ^| An error occurred. Please review the message above.          ^|
    echo   ^| Detailed logs (if any) are in the command window history.    ^|
    echo   +--------------------------------------------------------------+
    echo.
    pause
    exit /b 1


:: ============================================================================
:: Main Script Logic
:: ============================================================================
:main
cls
echo.
echo  ========================================================================
echo                           Data Launchpad Launcher
echo  ======================================================================
echo.

:: ----------------------------------------------------------------------------
call :log INFO "Step 1/6: Performing initial cleanup..."
:: This prevents "ghost" errors from zombie processes from previous runs by ensuring the port is free.
set "port=3000"
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :%port% ^| findstr LISTENING') do (
    set "pid=%%a"
    if defined pid (
        taskkill /F /PID %%a >nul
    )
)

:: ----------------------------------------------------------------------------
:: Cleaning Python cache...
:: This is a crucial step to prevent "ghost" errors from stale .pyc files.
:: It recursively finds and deletes all __pycache__ directories.
for /d /r "%SCRIPT_DIR%" %%d in (__pycache__) do (
    if exist "%%d" (
        rd /s /q "%%d"
    )
)

:: ----------------------------------------------------------------------------
call :log INFO "Step 2/6: Verifying system prerequisites..."
python --version >nul 2>&1
if %errorlevel% neq 0 (
    call :handle_error "Python is not installed or not in your system's PATH."
)

:: ----------------------------------------------------------------------------
:: Checking for Git...
git --version >nul 2>&1
if %errorlevel% neq 0 (
    call :handle_error "Git is not installed or not in your system's PATH."
)

:: ----------------------------------------------------------------------------
call :log INFO "Step 3/6: Setting up Python environment..."
:: Attempt to pull updates.
git pull >nul 2>&1
if %errorlevel% neq 0 (call :log INFO "Could not check for updates via Git. Using local version.")

:: ----------------------------------------------------------------------------
:: Verifying Python virtual environment...
if exist "%PYTHON_EXE%" (
    :: Venv exists, let's check its health. A broken pip is a common sign of corruption.
    "%PYTHON_EXE%" -m pip --version >nul 2>&1
    if %errorlevel% neq 0 (
        call :log WARNING "Virtual environment is corrupted (pip is not working). Removing..."
        rd /s /q "%VENV_DIR%"
        if exist "%VENV_DIR%" (
            call :handle_error "Failed to remove corrupted virtual environment. A file may be locked by another process. Please close other programs and try again."
        )
    )
)

if not exist "%PYTHON_EXE%" (
    call :log WARNING "Virtual environment not found or was corrupted. Creating it now..."
    python -m venv "%VENV_DIR%" >"%ERROR_LOG%" 2>&1
   if %errorlevel% neq 0 (
        echo.
        type "%ERROR_LOG%"
        call :handle_error "Failed to create the Python virtual environment."
    )
)

:: ----------------------------------------------------------------------------
:: Upgrading pip to prevent version notices...
:: This ensures the latest version of pip is used for installing packages.
"%PYTHON_EXE%" -m pip install --upgrade pip --quiet --no-color >nul


:: ----------------------------------------------------------------------------
:: Installing/updating required packages...
"%PYTHON_EXE%" -m pip install -r "%REQUIREMENTS_FILE%" --upgrade --quiet --no-color
if %errorlevel% neq 0 (
    echo.
    echo   Updating packages failed. Retrying with full output...
    echo   Please wait, this may take a moment.
    echo.
    "%PYTHON_EXE%" -m pip install -r "%REQUIREMENTS_FILE%" --upgrade
    if %errorlevel% neq 0 (
        call :handle_error "Failed to install required Python packages. See output above."
    )
)

:: Ensure openpyxl is installed for Excel support
"%PYTHON_EXE%" -m pip install openpyxl --upgrade --quiet --no-color

:: ----------------------------------------------------------------------------
call :log INFO "Step 4/6: Loading application configuration and credentials..."

:: Create desktop shortcut
set "SHORTCUT_PATH=%USERPROFILE%\Desktop\Launch Data Importer.lnk"
if not exist "%SHORTCUT_PATH%" (
    call :log INFO "Creating desktop shortcut..."
    (
        echo Set oWS = WScript.CreateObject^("WScript.Shell"^)
        echo sLinkFile = "%SHORTCUT_PATH%"
        echo Set oLink = oWS.CreateShortcut^(sLinkFile^)
        echo oLink.TargetPath = "%~f0"
        echo oLink.IconLocation = "imageres.dll, 71"
        echo oLink.Description = "Launch the Data Launchpad application"
        echo oLink.WorkingDirectory = "%SCRIPT_DIR%"
        echo oLink.Save
    ) >"%TEMP%\create_shortcut.vbs"
    cscript //nologo "%TEMP%\create_shortcut.vbs" >nul
)

:: Check for and read .env file
if not exist "%ENV_FILE%" (
    call :log WARNING "The .env file is missing. Creating a template..."
    (
        echo DB_SERVER=localhost
        echo DB_DATABASE=TargetDatabase
        echo DB_DRIVER=ODBC Driver 17 for SQL Server
        echo CREDENTIAL_TARGET=WindowsCredentialName
    ) > "%ENV_FILE%"
    call :handle_error "The .env file was missing. A template has been created. Please configure it and run again."
)

setlocal enabledelayedexpansion
for /f "usebackq tokens=1,* delims==" %%i in ("%ENV_FILE%") do (
    set "line=%%i"
    if /i "!line!"=="DB_SERVER" set "DB_SERVER=%%~j"
    if /i "!line!"=="DB_DATABASE" set "DB_DATABASE=%%~j"
    if /i "!line!"=="DB_DRIVER" set "DB_DRIVER=%%~j"
    if /i "!line!"=="CREDENTIAL_TARGET" set "CREDENTIAL_TARGET=%%~j"
)
endlocal & set "DB_SERVER=%DB_SERVER%" & set "DB_DATABASE=%DB_DATABASE%" & set "DB_DRIVER=%DB_DRIVER%" & set "CREDENTIAL_TARGET=%CREDENTIAL_TARGET%"

if not defined DB_SERVER call :handle_error "DB_SERVER is not defined in the .env file."
if not defined DB_DATABASE call :handle_error "DB_DATABASE is not defined in the .env file."
if not defined CREDENTIAL_TARGET call :handle_error "CREDENTIAL_TARGET is not defined in the .env file."

:: ----------------------------------------------------------------------------
:: Fetching database credentials securely...

set "DB_USERNAME="
set "DB_PASSWORD="
set "CREDS_OUTPUT=%TEMP%\creds_output.txt"

:: Run the script first to capture output and check for errors.
"%PYTHON_EXE%" "%GET_CREDS_SCRIPT%" --dotenv-path "%ENV_FILE%" > "%CREDS_OUTPUT%" 2> "%ERROR_LOG%"
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] The credential retrieval script failed.
    type "%ERROR_LOG%"
    if exist "%CREDS_OUTPUT%" del "%CREDS_OUTPUT%"
    call :handle_error "Failed to retrieve database credentials. See logs above."
)

for /f "tokens=1,* delims==" %%a in ('type "%CREDS_OUTPUT%"') do (
    set "%%a=%%b"
)
if exist "%CREDS_OUTPUT%" del "%CREDS_OUTPUT%"

if not defined DB_USERNAME (
    echo.
    type "%ERROR_LOG%"
    call :handle_error "Failed to retrieve database credentials. Check Windows Credential Manager setup."
)

:: ----------------------------------------------------------------------------
call :log INFO "Step 5/6: Preparing application resources..."

:: Use a block with setlocal to temporarily set environment variables
:: for just the create_dirs.py script. This is the most reliable way to pass them.
set "CREATE_DIRS_CMD="%PYTHON_EXE%" "%CREATE_DIRS_SCRIPT%""
cmd /c "set DB_USERNAME=%DB_USERNAME% && set DB_PASSWORD=%DB_PASSWORD% && %CREATE_DIRS_CMD%" >"%ERROR_LOG%" 2>&1

if %errorlevel% neq 0 (
    echo.
    type "%ERROR_LOG%"
    call :log WARNING "Failed to create monitored directories. Proceeding..."
)

:: ----------------------------------------------------------------------------
:: Configuring Dagster workspace...

set "WORKSPACE_YAML=%DAGSTER_HOME_DIR%\workspace.yaml"

:: Recreate the dagster_home directory to ensure a clean state.
if exist "%DAGSTER_HOME_DIR%" (
    rd /s /q "%DAGSTER_HOME_DIR%"
)
mkdir "%DAGSTER_HOME_DIR%"

:: Prepare the working directory path for YAML by replacing backslashes with forward slashes.
set "YAML_SAFE_SCRIPT_DIR=%SCRIPT_DIR:\=/%"

:: Always create a fresh workspace.yaml file.
(
    echo load_from:
    echo   - python_module:
    echo       module_name: "elt_project.elt_project.definitions"
    echo       location_name: "elt_project"
    echo       working_directory: "%YAML_SAFE_SCRIPT_DIR%"
) >"%WORKSPACE_YAML%"

:: Clean up any conflicting workspace files in the root directory to avoid confusion.
set "OLD_WORKSPACE_YAML=%SCRIPT_DIR%workspace.yaml"
if exist "%OLD_WORKSPACE_YAML%" (
    call :log INFO "Removing conflicting root workspace.yaml file."
    del "%OLD_WORKSPACE_YAML%"
)

:: ----------------------------------------------------------------------------
call :log INFO "Step 6/6: Launching Data Launchpad UI..."

:: Set the password in an environment variable that the UI process will inherit.
:: This is the crucial step to pass the credential to the separate UI process.
set "DAGSTER_DB_USERNAME=%DB_USERNAME%"
set "DAGSTER_DB_PASSWORD=%DB_PASSWORD%"
set "DAGSTER_HOME=%DAGSTER_HOME_DIR%"
set "PYTHONPATH=%SCRIPT_DIR%"

:: Launch the UI in a new background process, reliably passing environment variables.
:: We use pythonw.exe to run the script without a console window for a better UX.
:: The server will run silently in the background.
set "UI_CMD=%PYTHONW_EXE% %UI_SCRIPT% --server %DB_SERVER% --database %DB_DATABASE% --credential-target %CREDENTIAL_TARGET%"

:: ----------------------------------------------------------------------------

:: Start the UI process in the background FIRST.
start "Data Importer UI" %UI_CMD%

:: --- Wait for the server to be ready before opening the browser ---
:: This loop actively checks if the port is open, avoiding the "Connection Refused" error.
set "wait_time=0"
:wait_loop
:: Check if port 3000 is listening.
netstat -an | find "LISTENING" | find ":3000" >nul
if %errorlevel% equ 0 (
    goto :launch_browser
)
timeout /t 1 /nobreak >nul
set /a wait_time+=1
if %wait_time% lss 30 goto :wait_loop

call :handle_error "The server failed to start after 30 seconds."

:launch_browser
start http://localhost:3000
exit /b 0