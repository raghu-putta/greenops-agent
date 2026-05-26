@echo off
title GreenOps AI Dashboard
cd /d C:\Users\raghu\greenops-agent
call .venv\Scripts\activate
echo.
echo  =========================================
echo   Starting GreenOps AI Dashboard...
echo   Opening http://localhost:8000
echo  =========================================
echo.
start "" "http://localhost:8000"
uvicorn app:app --port 8000
