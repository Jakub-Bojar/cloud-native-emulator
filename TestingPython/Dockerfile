FROM python:3.12-slim
RUN pip install --no-cache-dir psutil prometheus_client
COPY load.py /load.py
CMD ["python", "-u", "/load.py"]