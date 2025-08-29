@echo off
setlocal

REM Activar el entorno virtual desde la raíz del proyecto
call C:\WORKSPACE\UPEU\cdk-datalake-ingest-upeu\.venv\Scripts\activate.bat

REM Movernos a la carpeta donde está el script y el txt
cd /c C:\WORKSPACE\UPEU\cdk-datalake-ingest-upeu\utils\extract_data

REM Iterar sobre las tablas del archivo tablas.txt
for /f %%T in (tables.txt) do (
    echo Procesando tabla %%T...
    python extract_data.py -t %%T
)

pause
