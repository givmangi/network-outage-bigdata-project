FROM python:3.10-slim
WORKDIR /app
COPY init_minio.py .
RUN pip install boto3
CMD ["python", "init_minio.py"]