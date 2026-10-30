import json
import time
import uuid
from kafka import KafkaProducer

def main():
    ejecucion = str(uuid.uuid4())

    productor = KafkaProducer(
        bootstrap_servers="localhost:9092",
        acks="all",
        value_serializer=lambda dato: json.dumps(
            dato, ensure_ascii=False
        ).encode("utf-8")
    )

    try:
        with open("datos/secop_2025.jsonl", encoding="utf-8") as archivo:
            for numero, linea in enumerate(archivo, start=1):
                if numero > 300:
                    break

                dato = json.loads(linea)
                dato["ejecucion_id"] = ejecucion
                dato["secuencia"] = numero

                resultado = productor.send(
                    "secop_contratos", value=dato
                ).get(timeout=30)

                print(
                    f"Enviado {numero}/300 | "
                    f"Partición: {resultado.partition} | "
                    f"Contrato: {dato.get('id_contrato')}",
                    flush=True
                )
                time.sleep(0.2)

        productor.flush()
        print("ENVIO TERMINADO. Ejecución:", ejecucion)
    finally:
        productor.close()

if __name__ == "__main__":
    main()
