FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install redis
COPY src/gds_shared ./gds_shared
COPY src/services/decomm_service/main.py ./main.py
CMD ["python", "main.py"]
