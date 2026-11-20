# Ejecutar en PowerShell desde PP_G002_SECOP_COMPLETO
$ErrorActionPreference = 'Stop'
Write-Host '1. Verificando servicios...'
docker compose ps
Write-Host '2. Verificando SQLite y exportando evidencias...'
docker compose exec -T worker python src/12_exportar_evidencias.py
if ($LASTEXITCODE -ne 0) { throw 'Fallo la exportacion' }
Write-Host '3. Generando tablero con graficas corregidas...'
docker compose exec -T worker python src/08_tablero.py
if ($LASTEXITCODE -ne 0) { throw 'Fallo el tablero' }
Write-Host '4. Copiando ZIP para Classroom...'
Write-Host 'Archivos disponibles en carpeta del proyecto: evidencias_finales_secop.zip y tablero_secop.html'
Write-Host 'Recuerda agregar capturas reales de Kafka, HDFS, MapReduce y reinicio de streaming al repositorio.'
