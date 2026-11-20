#!/bin/bash
docker start lambda26-kafka lambda26-kafka-ui lambda26-kafka-exporter lambda26-prometheus lambda26-pushgateway lambda26-grafana lambda26-pyspark
docker network connect lambda26-kafka-net lambda26-pyspark 2>/dev/null || true

sleep 4
docker exec -d -w /opt/UNIDAD2/Dim-Demanda-historica lambda26-pyspark python producers/producer_citibike_demanda.py

docker ps --filter "name=lambda26" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
