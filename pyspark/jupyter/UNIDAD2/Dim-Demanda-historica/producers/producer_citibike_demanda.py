import time, json, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Iniciando productor de eventos de demanda CitiBike...")
tipos_bici = ["classic_bike", "electric_bike"]
tipos_usuario = ["member", "casual"]

horas = list(range(0, 24))
dia = 1

while True:
    for hora in horas:
        # Simulación de curva de demanda real con picos a las 8h y 18h
        factor_hora = 2.5 if hora in [8, 9, 17, 18, 19] else (0.5 if hora in [1, 2, 3, 4] else 1.2)
        base_viajes = int(random.randint(120, 300) * factor_hora)
        
        ratio_member = 0.75 if hora in [8, 18] else 0.55
        viajes_member = int(base_viajes * ratio_member)
        viajes_casual = base_viajes - viajes_member
        
        evento = {
            "start_hour": hora,
            "day_of_week": (dia % 7) + 1,
            "is_weekend": 1 if ((dia % 7) + 1) in [1, 7] else 0,
            "member_casual": "member",
            "rideable_type": random.choice(tipos_bici),
            "trip_count": base_viajes,
            "viajes_member": viajes_member,
            "viajes_casual": viajes_casual,
            "timestamp": int(time.time() * 1000)
        }
        
        producer.send('citibike-viajes-eventos', evento)
        producer.flush()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Publicado intervalo Hora: {hora:02d}:00 | Demanda: {base_viajes} viajes")
        time.sleep(3)
    dia += 1
