
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path("/project")
DATA = ROOT / "data"
DATASET = "jbjy-vk9h"
API = "https://www.datos.gov.co/resource/{}.json".format(DATASET)

LIMITE = 1000
FECHA_INGESTA = datetime.now(timezone.utc).date().isoformat()

# Marca de agua existente
watermark_file = DATA / "marca_agua.json"

if not watermark_file.exists():
    raise FileNotFoundError(
        "Primero ejecuta 09_watermark.py"
    )

with open(watermark_file, encoding="utf-8") as f:
    watermark = json.load(f)

print("Marca de agua actual:", watermark)

# Leer fecha según la estructura del archivo
if isinstance(watermark, dict):
    marca = (
        watermark.get("watermark")
        or watermark.get("ultima_fecha")
        or watermark.get("fecha")
    )
else:
    marca = watermark

if not marca:
    raise ValueError("No se encontró una fecha válida en la marca de agua")

desde = (
    datetime.fromisoformat(str(marca)[:10])
    - timedelta(days=7)
).strftime("%Y-%m-%d")

sesion = requests.Session()
reintentos = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
sesion.mount("https://", HTTPAdapter(max_retries=reintentos))

parametros = {
    "$where": (
        "fecha_de_firma >= '{}T00:00:00' "
        "AND id_contrato IS NOT NULL"
    ).format(desde),
    "$order": "fecha_de_firma ASC, id_contrato ASC",
    "$limit": LIMITE,
    "$offset": 0
}

respuesta = sesion.get(API, params=parametros, timeout=90)
respuesta.raise_for_status()
registros = respuesta.json()

destino = DATA / "incremental" / (
    "fecha_ingesta={}".format(FECHA_INGESTA)
)
destino.mkdir(parents=True, exist_ok=True)

archivo = destino / "nuevos.jsonl"

ids = set()

with archivo.open("w", encoding="utf-8") as f:
    for registro in registros:
        identificador = str(registro["id_contrato"])

        if identificador in ids:
            continue

        ids.add(identificador)

        sobre = {
            "fuente": "SECOP_II",
            "dataset": DATASET,
            "id_registro": identificador,
            "fecha_ingesta": datetime.now(
                timezone.utc
            ).isoformat(),
            "payload": registro
        }

        f.write(json.dumps(sobre, ensure_ascii=False) + "\n")

print("Registros descargados:", len(registros))
print("Identificadores únicos:", len(ids))
print("Archivo generado:", archivo)
print("Carga incremental limitada a:", LIMITE)
print("Pendiente: integración idempotente con SQLite")
