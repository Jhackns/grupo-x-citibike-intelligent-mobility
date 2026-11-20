FROM python:3.11.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY productor_viajes.py productor_estaciones_gbfs.py ./
