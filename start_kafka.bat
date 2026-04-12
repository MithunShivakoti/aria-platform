@echo off
SET KAFKA_DIR=%~dp0kafka_2.13-3.7.0
SET LOG_DIR=C:\kafka-logs

REM Map to short drive to avoid classpath length limit
subst K: "%KAFKA_DIR%" 2>nul

echo ============================================
echo  Starting Kafka in KRaft mode (no Zookeeper)
echo ============================================

REM Step 1 — Generate cluster UUID
echo.
echo [1/3] Generating cluster UUID...
FOR /F "tokens=*" %%i IN ('K:\bin\windows\kafka-storage.bat random-uuid') DO SET KAFKA_UUID=%%i
echo UUID: %KAFKA_UUID%

REM Step 2 — Clear old logs and format storage fresh
echo.
echo [2/3] Formatting storage at %LOG_DIR%...
IF EXIST "%LOG_DIR%" rmdir /s /q "%LOG_DIR%"
mkdir "%LOG_DIR%"
K:\bin\windows\kafka-storage.bat format -t %KAFKA_UUID% -c K:\config\kraft\server.properties
echo Format step done (exit code: %ERRORLEVEL%)

REM Step 3 — Start broker in a new window
echo.
echo [3/3] Starting Kafka broker in new window...
powershell -command "Start-Process cmd -ArgumentList '/k K:\bin\windows\kafka-server-start.bat K:\config\kraft\server.properties' -WindowStyle Normal"
echo Waiting for broker to start (10 seconds)...
timeout /t 10 /nobreak

REM Step 4 — Create topic
echo.
echo Creating topic: sensor-readings...
K:\bin\windows\kafka-topics.bat --create --topic sensor-readings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo.
echo ============================================
echo  Kafka ready on localhost:9092
echo  Keep the Kafka window open!
echo ============================================
pause
