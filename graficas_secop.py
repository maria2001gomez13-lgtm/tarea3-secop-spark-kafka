import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

carpeta = Path("resultados/graficas")
carpeta.mkdir(parents=True, exist_ok=True)

def leer(nombre):
    filas = []
    for archivo in Path("resultados", nombre).glob("part-*.csv"):
        with archivo.open(encoding="utf-8", newline="") as entrada:
            filas.extend(csv.DictReader(entrada))
    if not filas:
        raise RuntimeError(f"No se encontraron resultados en {nombre}")
    return filas

plt.rcParams.update({"font.size": 12})

departamentos = sorted(
    leer("por_departamento"),
    key=lambda fila: int(fila["registros"]),
    reverse=True
)[:10]
departamentos.reverse()

fig, ax = plt.subplots(figsize=(12, 7))
barras = ax.barh(
    [fila["departamento"] for fila in departamentos],
    [int(fila["registros"]) for fila in departamentos],
    color="#005681"
)
ax.bar_label(barras, padding=5, fmt="%.0f")
ax.set_title("SECOP II: diez territorios con más registros en 2025")
ax.set_xlabel("Cantidad de registros")
ax.set_xlim(right=max(int(f["registros"]) for f in departamentos) * 1.2)
ax.xaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
fig.tight_layout()
fig.savefig(carpeta / "01_departamentos.png", dpi=180)
plt.close(fig)

meses = sorted(leer("por_mes"), key=lambda fila: fila["mes"])
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(
    [fila["mes"][5:] for fila in meses],
    [int(fila["registros"]) for fila in meses],
    marker="o",
    linewidth=2.5,
    color="#005681"
)
ax.set_title("SECOP II: registros por mes de firma en 2025")
ax.set_xlabel("Mes")
ax.set_ylabel("Cantidad de registros")
ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
ax.grid(axis="y", alpha=0.25)
fig.tight_layout()
fig.savefig(carpeta / "02_meses.png", dpi=180)
plt.close(fig)

print("GRAFICAS CREADAS EN:", carpeta.resolve())
