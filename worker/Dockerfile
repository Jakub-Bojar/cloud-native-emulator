FROM python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends stress-ng iperf3 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir prometheus-client==0.20.0 psutil==6.1.0
COPY worker.py .
EXPOSE 8080
ENV WORKER_PORT=8080
CMD ["python", "-u", "worker.py"]
