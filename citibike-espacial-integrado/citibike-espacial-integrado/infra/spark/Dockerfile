# Imagen de Spark + Jupyter para los notebooks U1/U2 y la ejecucion integrada U3
FROM python:3.10-slim-trixie

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir pyspark==4.2.0 notebook==7.6.2 pandas==2.3.3 pyarrow==21.0.0 matplotlib==3.10.7

WORKDIR /opt/proyecto
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
