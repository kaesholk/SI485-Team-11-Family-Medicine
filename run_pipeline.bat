@echo off
setlocal enabledelayedexpansion

REM Format the date to avoid invalid filename characters
for /f "tokens=1-3 delims=/" %%a in ("%date%") do (
    set month=%%a
    set day=%%b
    set year=%%c
)
set formatted_date=%year%-%month%-%day%

REM Define paths
set original_path=merged_data.csv
set elements_path=elements.csv
set scrape_path=scrape.csv
set altmetric_path=altmetric.csv
set merged_path=merged_data_%formatted_date%.csv
set keywords_path=articles_by_keyword.csv

REM Check for required files and folders
if not exist ".env" (
    echo [ERROR] .env file not found in current directory. Aborting pipeline.
    exit /b 1
)

if not exist "venv\" (
    echo [ERROR] venv/ not found in the current directory. Run install.bat to set up the environment.
    exit /b 1
)

echo [INFO] [%date% %time%] Starting pipeline...

python merge_datasets.py --original_csv %original_path% --elements_csv %elements_path% --scrape_csv %scrape_path% --altmetric_csv %altmetric_path% --output_csv %merged_path%
if errorlevel 1 (
    echo [ERROR] merge_datasets.py failed. Aborting.
    exit /b 1
)

python update_keywords_gpt.py --input_csv %merged_path% --output_csv %merged_path%
if errorlevel 1 (
    echo [ERROR] update_keywords_gpt.py failed. Aborting.
    exit /b 1
)

python reshape_keywords.py --input_csv %merged_path% --output_csv %keywords_path%
if errorlevel 1 (
    echo [ERROR] reshape_keywords.py failed. Aborting.
    exit /b 1
)

python clean_titles_gpt.py --input_csv %merged_path% --output_csv %merged_path%
if errorlevel 1 (
    echo [ERROR] clean_titles_gpt.py failed. Aborting.
    exit /b 1
)

echo [INFO] [%date% %time%] Pipeline finished successfully.
