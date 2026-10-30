import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
FILTRO = (
    "fecha_de_firma >= '2025-01-01T00:00:00' "
    "AND fecha_de_firma < '2026-01-01T00:00:00'"
)

def consultar(parametros):
    url = BASE + "?" + urllib.parse.urlencode(parametros)
    for intento in range(4):
        try:
            with urllib.request.urlopen(url, timeout=300) as respuesta:
                return json.load(respuesta)
        except Exception:
            if intento == 3:
                raise
            print("Reintentando conexión...", flush=True)
            time.sleep(3)

Path("datos").mkdir(exist_ok=True)
total = int(consultar({
    "$select": "count(*) as total",
    "$where": FILTRO
})[0]["total"])

print(f"Registros de 2025 disponibles: {total:,}", flush=True)

temporal = Path("datos/secop_2025.jsonl.part")
destino = Path("datos/secop_2025.jsonl")
descargados = 0

with temporal.open("w", encoding="utf-8") as archivo:
    while True:
        filas = consultar({
            "$select": (
                "id_contrato,departamento,tipo_de_contrato,"
                "fecha_de_firma,valor_del_contrato"
            ),
            "$where": FILTRO,
            "$order": ":id",
            "$limit": 20000,
            "$offset": descargados
        })
        if not filas:
            break
        for fila in filas:
            archivo.write(json.dumps(fila, ensure_ascii=False) + "\n")
        descargados += len(filas)
        print(f"Descargados: {descargados:,}", flush=True)

temporal.replace(destino)
print(f"DESCARGA TERMINADA: {descargados:,} registros.")
print(f"Archivo: {destino.resolve()}")
if descargados != total:
    print("Aviso: el total cambió durante la descarga; revisar.")
