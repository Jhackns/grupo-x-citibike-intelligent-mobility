# CitiBike Intelligent Mobility · Proyecto integrado U1 + U2 + U3 (Cristhian Chuquitarqui · Data Geniuses)
# Corre TODO con un solo comando, desde la carpeta del proyecto (Windows PowerShell):
#
#     powershell -ExecutionPolicy Bypass -File .\ejecutar.ps1
#
# Requisito: Docker Desktop abierto. La primera vez tarda ~25-30 min (imágenes, descarga de ~1 GB y el notebook completo).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Paso($texto) { Write-Host "`n=== $texto ===" -ForegroundColor Cyan }
function Revisar($que) { if ($LASTEXITCODE -ne 0) { Write-Host "ERROR en: $que" -ForegroundColor Red; exit 1 } }

Paso "1/4 Comprobando Docker"
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker no responde: abre Docker Desktop y vuelve a ejecutar." -ForegroundColor Red; exit 1 }
if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host "Creado .env desde .env.example" }

Paso "2/4 Levantando Kafka, productores, Spark/Jupyter, Prometheus y Grafana"
docker compose up -d --build --wait
Revisar "docker compose up (¿puertos ocupados por otro proyecto? ver README)"

Paso "3/4 Datos de julio 2026"
docker compose exec -T spark python datos/descargar_datos.py
Revisar "descarga de datos"
docker compose restart productor-viajes | Out-Null   # que el productor vea los CSV recién descargados

Paso "4/4 Notebook integrado U1 + U2 + U3 (~20 min; mientras tanto mira Grafana)"
Write-Host "  Dashboard general: http://localhost:43000/d/citibike-general-cristhian  (admin / admin)"
docker compose exec -T spark python scripts/streaming_en_segundo_plano.py --detener | Out-Null
docker compose exec -T spark jupyter nbconvert --to notebook --inplace --ExecutePreprocessor.timeout=-1 --execute notebook/CitiBike_Integrado_U1_U2_U3.ipynb
Revisar "notebook integrado"

Write-Host "`nLISTO." -ForegroundColor Green
Write-Host "  Dashboard general : http://localhost:43000/d/citibike-general-cristhian   (admin / admin)"
Write-Host "  Notebook          : http://localhost:4488/?token=sintoken  -> notebook/CitiBike_Integrado_U1_U2_U3.ipynb"
Write-Host "  El streaming sigue en vivo para Grafana. Para apagar todo: docker compose down"
