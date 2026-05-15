FROM python:3.12-slim
WORKDIR /app
COPY controller.py .
EXPOSE 8081
ENV CONTROLLER_PORT=8081
CMD ["python", "-u", "controller.py"]
